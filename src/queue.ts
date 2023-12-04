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
  concurrency = 0;
  worker: any;

  constructor(config: QueueConfig) {
    super();
    this.config = {
      redis: config.redis,
      queueName: config.queueName,
      keepOnFailure: config.keepOnFailure ?? true,
      keepOnSuccess: config.keepOnSuccess ?? true,
      retryDelay: config.retryDelay ?? 500,
    };
  }

  createQueueKey(key: string) {
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
    this.jobTick(); // Start processing
  }

  jobTick() {
    if (this.runningJobs >= this.concurrency) {
      // Maximum concurrency reached
      return;
    }

    this.runningJobs++;
    this.getNextJob()
      .then(async (jobId) => {
        if (!jobId) {
          this.runningJobs--;
          return;
        }

        const jobCreatedById = await new Job(this.config, null).fromId(jobId);
        if (jobCreatedById) {
          await this.executeJob(jobCreatedById);
        } else {
          console.error(`Job not found with ID: ${jobId}`);
        }
      })
      .catch((error) => {
        console.error("Error in jobTick:", error);
      })
      .finally(() => {
        this.runningJobs--;
        setImmediate(() => this.jobTick());
      });
  }

  private async executeJob<TJobPayload>(jobCreatedById: Job<TJobPayload>) {
    let hasError = false;
    try {
      await this.worker(jobCreatedById.data);
    } catch (error) {
      hasError = true;
    } finally {
      const [jobStatus, job] = await this.finishJob<TJobPayload>(jobCreatedById, hasError);
      this.emit(jobStatus, job.id);
      return;
    }
  }

  private async getNextJob() {
    try {
      const jobId = await this.config.redis.brpoplpush(
        this.createQueueKey("waiting"),
        this.createQueueKey("active"),
        0
      );
      return jobId;
    } catch (error) {
      console.error("Error fetching the next job:", error);
      this.runningJobs--;
      throw error;
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
        //TODO: Need polishing
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

  async removeJob(jobId: string) {
    const addJobToQueueScript = await Bun.file("./src/lua-scripts/remove-job.lua").text();
    return await this.config.redis.eval(
      addJobToQueueScript,
      5,
      this.createQueueKey("succeeded"),
      this.createQueueKey("failed"),
      this.createQueueKey("waiting"),
      this.createQueueKey("active"),
      this.createQueueKey("jobs"),
      jobId
    );
  }

  async destroy() {
    const args = ["id", "jobs", "waiting", "active", "succeeded", "failed"].map((key) =>
      this.createQueueKey(key)
    );
    const res = await this.config.redis.del(...args);
    return res;
  }
}
