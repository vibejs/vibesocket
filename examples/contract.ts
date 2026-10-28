import * as v from 'valibot';
import { defineSocketContract } from '../src/contract.ts';
export const roomSchema = v.pipe(v.string(), v.maxLength(100));
export const messageSchema = v.object({
  id: v.string(),
  room: roomSchema,
  text: v.pipe(v.string(), v.maxLength(2000)),
  user: v.string(),
  at: v.string(),
});
export const taskSchema = v.object({
  id: v.string(),
  title: v.string(),
  column: v.picklist(['todo', 'progress', 'done']),
});
export const boardSchema = v.object({ revision: v.number(), tasks: v.array(taskSchema) });
const sendSchema = v.object({
  room: roomSchema,
  text: v.pipe(v.string(), v.minLength(1), v.maxLength(2000)),
});
export const streamEvents = {
  metric: v.object({
    tick: v.number(),
    requests: v.number(),
    latency: v.number(),
    cpu: v.number(),
    at: v.string(),
  }),
  token: v.string(),
  done: v.boolean(),
};
export const demoContract = defineSocketContract({
  client: {
    'message:send': sendSchema,
    typing: v.object({ room: roomSchema, active: v.boolean() }),
  },
  server: {
    'message:created': messageSchema,
    'presence:updated': v.object({
      room: roomSchema,
      users: v.array(
        v.object({ userId: v.string(), connections: v.number(), lastActivity: v.number() }),
      ),
    }),
    'typing:updated': v.object({ room: roomSchema, users: v.array(v.string()) }),
    'board:updated': boardSchema,
  },
  requests: {
    'message:send': { input: sendSchema, output: messageSchema },
    history: { input: v.object({ room: roomSchema }), output: v.array(messageSchema) },
    board: { input: v.null(), output: boardSchema },
    'task:move': {
      input: v.object({
        id: v.string(),
        column: v.picklist(['todo', 'progress', 'done']),
        index: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(100)),
        expectedRevision: v.number(),
      }),
      output: boardSchema,
      errors: ['CONFLICT', 'NOT_FOUND'],
    },
    'task:create': {
      input: v.object({
        title: v.pipe(v.string(), v.minLength(1), v.maxLength(200)),
        expectedRevision: v.number(),
      }),
      output: boardSchema,
      errors: ['CONFLICT'],
    },
  },
});
