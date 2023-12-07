import { randomUUID } from "crypto";
import { Redis } from "ioredis";
import { convertToJSONString, formatMessageQueueKey } from "./utils";

type OwnerQueue = {
  redis: Redis;
  queueName: string;
};
export type JobStatuses = "created" | "waiting" | "active" | "succeeded" | "failed";

export class Job<T> {
  id: string;
  status: JobStatuses;
  config: OwnerQueue;
  data: T;

  constructor(ownerConfig: OwnerQueue, data: T, jobId = randomUUID()) {
    this.id = jobId;
    this.status = "created";
    this.data = data;
    this.config = ownerConfig;
  }

  private createQueueKey(key: string) {
    return formatMessageQueueKey(this.config.queueName, key);
  }

  fromId = async <T>(jobId: string): Promise<Job<T> | null> => {
    const jobData = await this.config.redis.hget(this.createQueueKey("jobs"), jobId);
    if (jobData) {
      return this.fromData<T>(jobId, jobData);
    }
    return null;
  };

  private fromData = <T>(jobId: string, stringifiedJobData: string): Job<T> => {
    const parsedData = JSON.parse(stringifiedJobData) as Job<T>;
    const job = new Job<T>(this.config, parsedData.data, jobId);
    job.status = parsedData.status;
    return job;
  };

  async save(): Promise<string | null> {
    const addJobToQueueScript = await Bun.file("./src/lua-scripts/add-job.lua").text();
    const resJobId = (await this.config.redis.eval(
      addJobToQueueScript,
      2,
      this.createQueueKey("jobs"),
      this.createQueueKey("waiting"),
      this.id,
      convertToJSONString(this.data, this.status)
    )) as string | null;

    if (resJobId) {
      this.id = resJobId;
      return resJobId;
    }
    return null;
  }
}
