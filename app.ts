import { createReadStream, createWriteStream, existsSync } from "node:fs";
import { pipeline } from "node:stream/promises";
import { createInterface } from "node:readline";
import { rm } from "fs/promises";
import { createFile } from "./createFile";

const TMP_FILE_MEMORY = 200_000_000;

const stringCount = 5000000;
const stringLength = 500;

const sortedFile = "sorted.txt";

(async () => {
  const filename = "random.txt";

  if (!existsSync(filename)) {
    await createFile(stringCount, stringLength, filename);
    console.timeEnd("createFile");
  }
  console.log("sorting in progress..");
  console.time("sort");

  await sort(filename);
})();

async function sort(filename: string) {
  const file = createReadStream(filename);
  const lines = createInterface({
    input: file,
  });

  const stringsBucket = [];

  let size = 0;
  const tmpSortFiles: string[] = [];

  for await (const line of lines) {
    size += line.length;
    stringsBucket.push(line);

    if (size > TMP_FILE_MEMORY) {
      await sortAndWriteToFile(stringsBucket, tmpSortFiles);
      size = 0;
    }
  }

  if (stringsBucket.length != 0) {
    await sortAndWriteToFile(stringsBucket, tmpSortFiles);
  }

  await mergeTmpFiles(tmpSortFiles, sortedFile);
  tmpSortFiles.forEach((f) => rm(f));
}

async function mergeTmpFiles(sortFileNames: string[], fileName: string) {
  const file = createWriteStream(fileName);
  const readers = sortFileNames.map((name) =>
    createInterface({
      input: createReadStream(name),
    })[Symbol.asyncIterator]()
  );

  return pipeline(sortedStringsFromTmpFiles(readers), file);
}

async function* sortedStringsFromTmpFiles(
  readers: AsyncIterableIterator<string>[]
) {
  const values = await Promise.all(
    readers.map((r) => r.next().then((res) => res.value))
  );

  while (readers.length > 0) {
    const [minVal, i] = values.reduce(
      (prev, cur, index) => (cur < prev[0] ? [cur, index] : prev),
      [values[0], 0]
    );

    yield `${minVal}\n`;
    const res = await readers[i].next();
    if (!res.done) {
      values[i] = res.value;
      continue;
    }

    values.splice(i, 1);
    readers.splice(i, 1);
  }
}

async function sortAndWriteToFile(
  stringsBucket: string[],
  tmpSortFiles: string[]
) {
  stringsBucket.sort();

  const sortFileName = `sort_${tmpSortFiles.length}.tmp`;
  tmpSortFiles.push(sortFileName);

  await pipeline(
    stringsBucket.map((e) => `${e}\n`),
    createWriteStream(sortFileName)
  );

  stringsBucket.length = 0;
}

process.on("beforeExit", () => console.timeEnd("sort"));
