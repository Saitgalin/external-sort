import { createWriteStream } from "fs";
import { randomString } from "./utils";
import { pipeline } from "stream/promises";

export const createFile = (stringCount: number, stringLength: number, filename: string) => {
  
  console.time("createFile");
  console.log("Generating file...");

  const writeStream = createWriteStream(filename);
  let counter = 0;

  return pipeline(function* () {
    while (counter < stringCount) {
      let r = randomString(stringLength) + "\n";
      ++counter;
      yield r;
    }
  }, writeStream);
};
