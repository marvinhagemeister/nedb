export interface Task {
  this: any;
  fn: Function;
  arguments: any;
}

export default class Queue<T = any> {
  private queue: T[] = [];
  private isRunning = false;

  constructor(private fn: (item: T) => void | Promise<void>) {}

  private async _run() {
    this.isRunning = true;

    for (const item of this.queue) {
      await this.fn(item);
    }

    this.isRunning = false;
  }

  push(item: T) {
    this.queue.push(item);
    if (!this.isRunning) this._run();
  }
}
