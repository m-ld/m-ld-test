import { ChildProcess } from 'child_process';
import type { DirResult } from 'tmp';

export interface Orchestrator {
  close(): Promise<void>;
  on(
    event: 'listening',
    handler: (url: string) => void
  ): this;
}

export interface CloneChildProcessState {
  id: string,
  process?: ChildProcess,
  tmpDir: DirResult
}

export type CloneRemotingInfo<ProcessState extends CloneChildProcessState> = {
  config: {}, meta: Omit<ProcessState, keyof CloneChildProcessState>
};

export interface Remoting<ProcessState extends CloneChildProcessState> {
  initialise(clones: { [id: string]: ProcessState }): void;
  provision(cloneId: string): Promise<CloneRemotingInfo<ProcessState>>;
  partition(clone: ProcessState, partition: boolean): Promise<void>;
  release(clone: ProcessState, opts?: { unref: true }): Promise<void>;
}

export * from './CloneProcess';
export * from './Forkestrator';
