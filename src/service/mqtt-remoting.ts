import { CloneChildProcess, Remoting } from './index';
import Aedes from 'aedes';
import type { Client } from 'aedes:client';
import LOG from 'loglevel';
import { logTs } from '../util';
import { promisify } from 'util';
import { AddressInfo, createServer, Server } from 'net';

export interface MqttCloneChildProcess extends CloneChildProcess {
  mqtt: {
    client?: Client,
    server: Server,
    port: number
  };
}

export class MqttRemoting implements Remoting<MqttCloneChildProcess> {
  private readonly aedes = new Aedes();

  initialise(clones: {
    [cloneId: string]: MqttCloneChildProcess
  }) {
    this.aedes.on('publish', (packet, client) => {
      const log = LOG.getLogger('aedes');
      if (client) {
        const { topic, qos, retain } = packet;
        log.debug(
          logTs(), client.id, { topic, qos, retain },
          log.getLevel() <= LOG.levels.TRACE ? packet.payload.toString() : '');
      }
    });

    this.aedes.on('client', client => {
      const log = LOG.getLogger('aedes');
      if (client.id in clones) {
        log.debug(logTs(), client.id, 'MQTT client connecting');
        clones[client.id].mqtt.client = client;
      } else {
        log.warn(logTs(), client.id, 'Unexpected MQTT client');
      }
    });

    function reportError(client: Client, err: any) {
      // Don't report if the clone is dead or dying
      const cloneProcess = clones[client.id]?.process;
      if (cloneProcess != null && !cloneProcess.killed)
        LOG.getLogger('aedes').warn(client.id, err);
    }
    this.aedes.on('clientError', reportError);
    this.aedes.on('connectionError', reportError);
  }

  provision(cloneId: string) {
    // @ts-ignore - no idea
    const mqttServer = createServer(this.aedes.handle);
    return new Promise<{
      config: {},
      meta: Omit<MqttCloneChildProcess, keyof CloneChildProcess>
    }>((resolve, reject) => {
      mqttServer.listen((err: any) => {
        if (err)
          return reject(err);
        const mqttPort = (mqttServer.address() as AddressInfo).port;
        LOG.debug(logTs(), cloneId, `Clone MQTT port is ${mqttPort}`);
        return resolve({
          config: {
            mqtt: {
              host: 'localhost',
              port: mqttPort,
              // Short timeouts as everything is local
              connectTimeout: 100,
              keepalive: 1
            }
          },
          meta: {
            mqtt: { server: mqttServer, port: mqttPort }
          }
        });
      });
    });
  }

  partition({ id, mqtt }: MqttCloneChildProcess, partition: boolean) {
    return new Promise<void>((resolve, reject) => {
      if (partition && mqtt.server.listening) {
        if (mqtt.client)
          mqtt.client.conn.destroy();
        mqtt.server.close(err => {
          if (err) {
            return reject(err);
          } else {
            LOG.debug(logTs(), id, `MQTT stopped`);
            return resolve();
          }
        });
      } else if (!partition && !mqtt.server.listening) {
        mqtt.server.listen(mqtt.port, () => {
          LOG.debug(logTs(), id, `MQTT re-started`);
          return resolve();
        });
      } else {
        return reject('Partition request does not match MQTT state');
      }
    });
  }

  release(
    { id, mqtt }: MqttCloneChildProcess,
    opts?: { unref: true }
  ) {
    return new Promise<void>((resolve, reject) => {
      if (mqtt.server.listening) {
        // Give the broker a chance to shut down. If it does not, this usually
        // indicates that the clone has not released its connection. In that case
        // unref the server to allow this process to exit and signal the error,
        // unless opts.unref is set.
        Promise.race([
          promisify(mqtt.server.close).call(mqtt.server),
          new Promise(fin => setTimeout(fin, 1000, 'timed out'))
        ]).then(result => {
          LOG.debug(logTs(), id, 'Clone broker shutdown', result || 'OK');
          if (result === 'timed out') {
            if (!opts?.unref)
              reject(result);
            mqtt.server.unref();
          } else {
            resolve(); // Closed OK
          }
        }).catch(reject);
      } else {
        resolve();
      }
    });
  }
}
