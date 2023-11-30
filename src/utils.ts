import Redis from "ioredis";
import { Job, JobStatuses } from "./Job";

const MQ_PREFIX = "UpstashMQ";

export const formatMessageQueueKey = (queueName: string, key: string) => {
  return `${MQ_PREFIX}:${queueName}:${key}`;
};

export const convertToJSONString = <T>(data: T, status: JobStatuses): string => {
  return JSON.stringify({
    data,
    status,
  });
};

export const delay = (duration: number): Promise<void> => {
  return new Promise((resolve) => {
    setTimeout(resolve, duration);
  });
};
