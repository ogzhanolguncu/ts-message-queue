import Redis from "ioredis";
import { EventEmitter } from "events";
import { convertToJSONString, delay, formatMessageQueueKey } from "./utils";
import { Job, JobStatuses } from "./job";

const MAX_REDIS_FAILURE_RETRY_DELAY_IN_MS = 30000;
const MAX_RETRIES = 5;

export type QueueConfig = {
  redis: Redis;
  queueName: string;
  /**
   * ```ts
   * keepOnSuccess: true
   * ```
   * Retry in milliseconds for failure during Redis fetch
   * @default 500
   * */
  retryDelay?: number;
  /**
   * ```ts
   * keepOnSuccess: true
   * ```
   * This allows you to keep data in Redis after successfully processing them
   * @default true
   * */
  keepOnSuccess?: boolean;
  /**
   * ```ts
   * keepOnFailure: true
   * ```
   * This allows you to keep data in Redis after failling gracefully during the processing
   * @default true
   * */
  keepOnFailure?: boolean;
};

type Worker = <T>(job: T) => Promise<void>;

export class Queue extends EventEmitter {
  config: QueueConfig;

  runningJobs = 0;
  queuedJobs = 0;
  concurrency = 0;

  worker: any;

  constructor(config: QueueConfig) {
    super();
    this.config = {
      redis: config.redis,
      queueName: config.queueName,
      keepOnFailure: true,
      keepOnSuccess: true,
      retryDelay: 500,
    };
  }

  private createQueueKey(key: string) {
    return formatMessageQueueKey(this.config.queueName, key);
  }

  async add<T>(payload: T) {
    return new Job<T>(this.config, payload).save();
  }

  async process<TJobPayload>(
    worker: (job: TJobPayload) => void,
    concurrency: number
  ): Promise<void> {
    this.concurrency = concurrency;
    this.worker = worker;

    this.runningJobs = 0;
    this.queuedJobs = 0;
    this.fillJobQueue<TJobPayload>();
  }

  private fillJobQueue<TJobPayload>() {
    while (this.queuedJobs < this.concurrency - this.runningJobs) {
      this.queuedJobs++;
      console.log("JOB QUEUE STARTS");
      setImmediate(() => this.processNextJob<TJobPayload>());
    }
  }

  private async processNextJob<TJobPayload>() {
    try {
      const jobId = await this.getNextJob();
      if (jobId) {
        //TODO: This might fail handle this carefully.
        const jobCreatedById = await new Job<TJobPayload | null>(
          this.config,
          null
        ).fromId<TJobPayload>(jobId);
        if (!jobCreatedById) {
          console.error(`Job not found with ID: ${jobId}`);
          return;
        }

        let hasError = false;
        try {
          await this.worker(jobCreatedById.data);
        } catch (error) {
          hasError = true;
        } finally {
          //TODO: This might fail handle this
          const [jobStatus, job] = await this.finishJob<TJobPayload>(jobCreatedById, hasError);
          this.emit(jobStatus, job.id);
          this.runningJobs--;
          return;
        }
      }
    } catch (error) {
      this.emit("error", (error as Error).message);

      if (MAX_REDIS_FAILURE_RETRY_DELAY_IN_MS === this.config.retryDelay) return null;
      this.config.retryDelay = this.config.retryDelay! * 2;
      console.log("retrying here");
      return delay(this.config.retryDelay).then(() => this.getNextJob());
    } finally {
      this.runningJobs--;
      this.fillJobQueue(); // Trigger fetching the next job
    }
  }

  private async getNextJob() {
    try {
      const jobId = await this.config.redis.brpoplpush(
        this.createQueueKey("waiting"),
        this.createQueueKey("active"),
        0
      );

      if (!jobId) {
        this.queuedJobs--;
        this.runningJobs--;
        console.log("No job available in the queue.");
        return null;
      }

      this.runningJobs++;
      this.queuedJobs--;

      return jobId;
    } catch (error) {
      console.error("Error fetching the next job:", error);
      this.runningJobs--;
      throw error; // Re-throw or handle the error as per your application logic
    }
  }

  private async finishJob<TJobPayload>(
    job: Job<TJobPayload>,
    hasFailed?: boolean
  ): Promise<[JobStatuses, Job<TJobPayload>]> {
    const multi = this.config.redis.multi();

    // Remove from active queue
    multi
      .lrem(this.createQueueKey("active"), 0, job.id)
      .srem(this.createQueueKey("stalling"), job.id);

    if (hasFailed) {
      job.retryCount = job.retryCount + 1;
      if (job.retryCount <= MAX_RETRIES) {
        delay(500).then(() => multi.rpush(this.createQueueKey("waiting"), JSON.stringify(job)));
      } else {
        job.status = "failed";
        if (this.config.keepOnFailure) {
          multi.hset(
            this.createQueueKey("jobs"),
            job.id,
            convertToJSONString(job.data, job.status)
          );
          multi.sadd(this.createQueueKey("failed"), job.id);
        } else {
          multi.hdel(this.createQueueKey("jobs"), job.id);
        }
      }
    } else {
      if (this.config.keepOnSuccess) {
        multi.hset(this.createQueueKey("jobs"), job.id, convertToJSONString(job.data, job.status));
        multi.sadd(this.createQueueKey("succeeded"), job.id);
      } else {
        multi.hdel(this.createQueueKey("jobs"), job.id);
      }
      job.status = "succeeded";
    }

    await multi.exec();
    return [job.status, job];
  }
}
