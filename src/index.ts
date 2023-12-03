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
  try {
    await generateQueueItems(queue, 10);

    // queue.on("succeeded", (result) => {
    //   console.log(`Received result for job: ${result}`);
    // });
    async function myWorkerFunction(job: Payload) {
      console.log("Processing job:", job.data);
    }
    await sleep(2000);
    await queue.process<Payload>(myWorkerFunction, 1);
  } catch (error) {
    console.error("Error occurred:", error);
    // Handle the error appropriately
  }
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
    await queue.add(payload);
    console.log(`Added item ${i} to queue`);
  }
}
