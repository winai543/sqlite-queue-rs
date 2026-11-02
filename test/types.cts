import { SQLiteQueue } from 'sqlite-queue-rs';
const store = new SQLiteQueue(':memory:');
const queue = store.queue<string>('test');
const id: string = queue.push('hello');
queue.cancel(id);
store.close();
