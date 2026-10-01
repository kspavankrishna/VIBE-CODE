import fs from "node:fs";
import process from "node:process";

export const readTextFileImpl = (path) => () => {
  if (path === "-") {
    return fs.readFileSync(0, "utf8");
  }
  return fs.readFileSync(path, "utf8");
};

export const fileExists = (path) => () => {
  try {
    return fs.statSync(path).isFile();
  } catch (_) {
    return false;
  }
};

export const argv = () => process.argv.slice(2);

export const writeStdout = (text) => () => {
  process.stdout.write(text);
};

export const writeStderr = (text) => () => {
  process.stderr.write(text);
};

export const exitWith = (code) => () => {
  process.exitCode = code;
};
