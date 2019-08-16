/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

import * as fs from "fs";
import * as path from "path";
import { promisify } from "util";

import { exists, writeFile, remove, open, close } from "nicer-fs";
export { exists, writeFile, readFile, mkdir } from "nicer-fs";
export const rename = promisify(fs.rename);
export const unlink = promisify(fs.unlink);
export const appendFile = promisify(fs.appendFile);

const fsync = promisify(fs.fsync);

/** Explicit name ... */
export async function ensureFileDoesntExist(file: string) {
  const res = await exists(file);
  if (!res) return;
  return remove(file);
}

export interface FlushOptions {
  filename: string;
  isDir?: boolean;
}

/**
 * Flush data in OS buffer to storage if corresponding option is set
 * If options is a string, it is assumed that the flush of the file (not dir)
 * called options was requested
 */
export async function flushToStorage(options: string | FlushOptions) {
  var filename, flags;
  if (typeof options === "string") {
    filename = options;
    flags = "r+";
  } else {
    filename = options.filename;
    flags = options.isDir ? "r" : "r+";
  }

  // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
  // except in the very rare event of the first time database is loaded and a crash happens
  if (flags === "r" && process.platform === "win32") {
    return;
  }

  const fd = await open(filename, flags);
  try {
    await fsync(fd);
    await close(fd);
  } catch (err) {
    var e = new Error("Failed to flush to storage");
    (e as any).errorOnFsync = err;
    (e as any).errorOnClose = err;
    throw e;
  }
}

/**
 * Fully write or rewrite the datafile, immune to crashes during the write
 * operation (data will not be lost)
 */
export async function crashSafeWriteFile(filename: string, data: string) {
  const tempFilename = filename + "~";

  await flushToStorage({
    filename: path.dirname(filename),
    isDir: true,
  });

  const res = await exists(filename);
  if (res) {
    await flushToStorage(filename);
  }

  await writeFile(tempFilename, data);
  await flushToStorage(tempFilename);
  await rename(tempFilename, filename);
  await flushToStorage({
    filename: path.dirname(filename),
    isDir: true,
  });
}

/**
 * Ensure the datafile contains all the data, even if there was a crash during a
 * full file write
 */
export async function ensureDatafileIntegrity(filename: string) {
  var tempFilename = filename + "~";

  const filenameExists = await exists(filename);
  // Write was successful
  if (filenameExists) return;

  const oldFilenameExists = await exists(tempFilename);
  // New database
  if (!oldFilenameExists) {
    return writeFile(filename, "", "utf8");
  }

  // Write failed, use old version
  return rename(tempFilename, filename);
}
