import Queue, { Task } from "./Queue";

var async = require("async");

/** Responsible for sequentially executing actions on the database */
export default class Executor {
  private buffer: any[] = [];
  public ready = false;
  private queue: Queue<Task>;

  constructor() {
    // This queue will execute all commands, one-by-one in order
    this.queue = new Queue(task => {
      var newArguments = [];

      // task.arguments is an array-like object on which adding a new field doesn't work, so we transform it into a real array
      for (var i = 0; i < task.arguments.length; i += 1) {
        newArguments.push(task.arguments[i]);
      }
      var lastArg = task.arguments[task.arguments.length - 1];

      // Always tell the queue task is complete. Execute callback if any was given.
      if (typeof lastArg === "function") {
        // Callback was supplied
        newArguments[newArguments.length - 1] = function() {
          if (typeof setImmediate === "function") {
            setImmediate(cb);
          } else {
            process.nextTick(cb);
          }
          lastArg.apply(null, arguments);
        };
      } else if (!lastArg && task.arguments.length !== 0) {
        // false/undefined/null supplied as callbback
        newArguments[newArguments.length - 1] = function() {
          cb();
        };
      } else {
        // Nothing supplied as callback
        newArguments.push(function() {
          cb();
        });
      }

      task.fn.apply(task.this, newArguments);
    });
  }

  /**
   * If executor is ready, queue task (and process it immediately if executor was idle)
   * If not, buffer task for later processing
   * @param {Object} task
   *                 task.this - Object to use as this
   *                 task.fn - Function to execute
   *                 task.arguments - Array of arguments, IMPORTANT: only the last argument may be a function (the callback)
   *                                                                 and the last argument cannot be false/undefined/null
   * @param {Boolean} forceQueuing Optional (defaults to false) force executor to queue task even if it is not ready
   */
  push(task: Task, forceQueuing: boolean = false) {
    if (this.ready || forceQueuing) {
      this.queue.push(task);
    } else {
      this.buffer.push(task);
    }
  }

  /**
   * Queue all tasks in buffer (in the same order they came in)
   * Automatically sets executor as ready
   */
  processBuffer() {
    this.ready = true;
    for (const item of this.buffer) {
      this.queue.push(item);
    }
    this.buffer = [];
  }
}
