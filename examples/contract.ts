import * as v from 'valibot';
import { defineSocketContract } from '../src/contract.ts';
export const messageSchema = v.object({ id: v.string(), room: v.string(), text: v.pipe(v.string(), v.maxLength(2000)), user: v.string(), at: v.string() });
export const demoContract = defineSocketContract({
  client: { 'message:send': v.object({ room: v.string(), text: v.pipe(v.string(), v.minLength(1), v.maxLength(2000)) }) },
  server: { 'message:created': messageSchema },
});
