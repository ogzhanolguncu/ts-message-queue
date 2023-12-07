import { sleep } from "bun";
import Redis from "ioredis";
import { Queue } from "./queue";

type Payload = {
  id: number;
  data: string;
};

const queue = new Queue({
  redis: new Redis(process.env.UPSTASH_REDIS_URL!),
  queueName: "mytest-queue",
});

async function main() {
  await generateQueueItems(queue, 20);
  console.log("Sleep starting 5 sec");
  await sleep(5000);

  await queue.process<Payload>((job) => {
    console.log("Processing job:", job.data);
    sleep(1000);
  }, 3);
}

main();

async function generateQueueItems(queue: Queue, itemCount: number) {
  for (let i = 0; i < itemCount; i++) {
    const payload = {
      id: i,
      data: `dummy-data-${i}`,
      // Add more properties as needed for your testing
    };
    // await sleep(1000);
    const jobId = await queue.add(payload);
    console.log(`Added item ${i} with jobId: ${jobId}`);
  }
}
