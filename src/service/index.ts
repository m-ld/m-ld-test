import { ChildProcess } from 'child_process';
import type { DirResult } from 'tmp';

export interface Orchestrator {
  close(): Promise<void>;
  on(
    event: 'listening',
    handler: (url: string) => void
  ): this;
}

export interface CloneChildProcess {
  id: string,
  process?: ChildProcess,
  tmpDir: DirResult
}

export interface Remoting<ProcessType extends CloneChildProcess> {
  initialise(clones: { [id: string]: ProcessType }): void;
  provision(cloneId: string): Promise<{
    config: {}, meta: Omit<ProcessType, keyof CloneChildProcess>
  }>;
  partition(clone: ProcessType, partition: boolean): Promise<void>;
  release(clone: ProcessType, opts?: { unref: true }): Promise<void>;
}

export * from './clone-process';
export * from './forkestrator';
export * from './mqtt-remoting';