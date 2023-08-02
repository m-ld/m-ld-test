import type { ChildProcess } from 'child_process';
import type { Server } from 'net';
import type { Client } from 'aedes:client';
import type { DirResult } from 'tmp';
import type { MeldClone, MeldConfig } from '@m-ld/m-ld-spec';

declare namespace m_ld {
  type CloneFactory = (config: MeldConfig, tmpDirName: string) =>
    Promise<MeldClone & { close(): Promise<void> }>;
}

declare namespace m_ld_test {
  interface Orchestrator {
    close(): Promise<void>;
    on(
      event: 'listening',
      handler: (url: string) => void
    ): this;
  }

  interface CloneChildProcess {
    id: string,
    process: ChildProcess,
    tmpDir: DirResult
  }

  interface Remoting<ProcessType extends CloneChildProcess> {
    initialise(clones: { [id: string]: ProcessType }): void;
    provision(cloneId: string): Promise<{
      config: {},
      meta: Omit<ProcessType, keyof CloneChildProcess>
    }>;
    partition(clone: ProcessType, partition: boolean): Promise<void>;
    release(clone: ProcessType, opts?: { unref: true }): Promise<void>;
  }

  interface StartMessage {
    '@type': 'start';
    config: MeldConfig & { logLevel: import('loglevel').LogLevelDesc };
    tmpDirName: string;
    requestId: string;
  }

  // noinspection JSUnusedGlobalSymbols TODO: and other messages
  interface ErrorMessage {
    '@type': 'error';
    err: string;
  }

  interface MqttCloneChildProcess extends CloneChildProcess {
    mqtt: {
      client: Client,
      server: Server,
      port: number
    };
  }
}

declare namespace aedes {
  type Aedes = import('aedes').Aedes;
}
