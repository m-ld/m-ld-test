// noinspection JSUnusedGlobalSymbols - TODO: testing

import path from 'path';
import { DirResult, dirSync } from 'tmp';
import { EventEmitter, once } from 'events';
import { mkdirSync } from 'fs';
import { ChildProcess, fork, ForkOptions, spawn, SpawnOptions } from 'child_process';
import { Readable } from 'stream';

export type OutMatcher = string | RegExp | ((out: string) => string[] | null);

export type FindOptions = {
  matchError?: OutMatcher,
  timeout?: number
};

class CmdProcess extends EventEmitter {
  exitCode: number | null = null;
  buffer = '';
  kill: ChildProcess['kill'];
  input: (text: string) => void;

  constructor(process: ChildProcess) {
    super();
    process.on('spawn', () => this.emit('spawn'));
    process.on('exit', (exitCode, signal) => {
      this.exitCode = exitCode;
      if (!this.buffer.endsWith('\n'))
        this.emit('line', this.buffer.slice(this.buffer.lastIndexOf('\n') + 1));
      this.emit('exit', exitCode, signal);
    });
    this.input = text => {
      process.stdin!.write(text + '\n');
    };
    const captureOut = (readable: Readable) =>
      readable.on('readable', () => {
        const prevLf = this.buffer.lastIndexOf('\n');
        let chunk;
        while (null !== (chunk = readable.read()))
          this.buffer += chunk;
        const lines = this.buffer.slice(prevLf + 1).split('\n');
        for (let i = 0; i < lines.length; i++) {
          i < lines.length - 1 && this.emit('line', lines[i]);
          lines[i] && this.emit('out', lines[i]);
        }
      });
    captureOut(process.stdout!);
    captureOut(process.stderr!);
    this.kill = process.kill.bind(process);
  }

  async find(
    match: OutMatcher,
    { matchError = 'MeldError', timeout = Infinity }: FindOptions = {}
  ) {
    const matchFn = this.matcher(match);
    const matchErrorFn = this.matcher(matchError);
    return this.query(matchFn) ?? await new Promise<string[]>((resolved, rejected) => {
      const foundListener = (out: string) => {
        const result = matchFn(out);
        if (result)
          settle(resolved, result);
        else if (matchErrorFn(out))
          settle(rejected, out);
      };
      this.on('out', foundListener);
      const timer = timeout < Infinity &&
        setTimeout(() => settle(rejected, 'timed out'), timeout);
      const exitListener = () => settle(rejected, `process exited:\n${this.buffer}`);
      this.on('exit', exitListener);
      const settle = (fn: typeof resolved | typeof rejected, ...arg: Parameters<typeof fn>) => {
        timer !== false && clearTimeout(timer);
        this.off('out', foundListener);
        this.off('exit', exitListener);
        fn.apply(null, arg);
      };
    });
  }

  query(match: OutMatcher) {
    const matchFn = this.matcher(match);
    const lines = this.buffer.split('\n');
    for (let i = lines.length - 1; i >= 0; i--) {
      const result = matchFn(lines[i]);
      if (result)
        return result;
    }
    return null;
  }

  matcher(match: OutMatcher) {
    if (typeof match == 'function') {
      return match;
    } else if (typeof match == 'string') {
      return (line: string) => {
        const i = line.indexOf(match);
        return i > -1 ? [
          line,
          line.substring(0, i),
          match,
          line.substring(i + match.length)
        ] : null;
      };
    } else {
      return (line: string) => match.exec(line);
    }
  }

  clear() {
    this.buffer = '';
  }
}

export class Cmd {
  static logging = false;
  static wd = 'test';

  public readonly name: string;

  protected process?: CmdProcess;
  protected log: Console['log'];
  protected childLog: Console['log'];
  protected readonly rootDir: string;

  private dirs: DirResult[] = [];

  constructor(...name: string[]) {
    this.rootDir = path.resolve(process.cwd(), path.join(Cmd.wd, ...name));
    this.name = name.join(' ');
    this.console = Cmd.logging ? console : null;
    this.childConsole = null;
  }

  set console(console: Console | null) {
    this.log = console ? console.log.bind(console, this.name) : () => {};
  }

  set childConsole(console: Console | null) {
    this.childLog = console ? console.log.bind(console) : () => {};
  }

  createDir(purpose: string) {
    mkdirSync(this.rootDir, { recursive: true });
    // noinspection JSCheckFunctionSignatures
    const dir = dirSync({
      unsafeCleanup: true,
      tmpdir: this.rootDir
    });
    this.dirs.push(dir);
    this.log('Created dir',
      dir.name.replace(process.cwd(), '.'),
      ...(purpose ? ['for', purpose] : []));
    return dir.name;
  }

  run = (...args: (string | Partial<ForkOptions>)[]) => this.exec(fork, ...args);
  fork = this.run;
  spawn = (...args: (string | Partial<SpawnOptions>)[]) => this.exec(spawn, ...args);

  private async exec<Opts>(
    exec: (cmd: string, args: string[], opts: Opts) => ChildProcess,
    ...args: (string | Partial<Opts>)[]
  ) {
    if (this.process)
      throw new RangeError('Already running');
    const [modulePath, ...argv] =
      args.filter<string>((a): a is string => typeof a == 'string');
    // noinspection JSCheckFunctionSignatures
    const opts: Opts = Object.assign({
      cwd: this.rootDir, silent: true, env: process.env
    }, ...args.filter(a => typeof a == 'object'));
    this.process = new CmdProcess(exec(modulePath, argv, opts));
    await once(this.process, 'spawn');
    this.process.on('line', this.childLog);
    this.process.on('exit', code => this.log('exited', code));
    return opts;
  }

  get running() {
    if (this.process == null)
      throw new RangeError('Not running');
    return this.process;
  }

  async findByText(text: OutMatcher, opts?: FindOptions) {
    const result = await this.running.find(text, opts);
    this.log('→', result[0]);
    return result;
  }

  async findByPrefix(prefix: string) {
    const [, , , suffix] = await this.findByText(prefix);
    return suffix.trim();
  }

  queryByText(text: OutMatcher) {
    return this.running.query(text);
  }

  type(text: string) {
    this.clear(); // We always want following output
    this.running.input(text);
    this.log('←', text);
  }

  getOut() {
    return this.running.buffer;
  }

  async waitForExit() {
    if (this.process != null) {
      if (this.process.exitCode == null)
        await once(this.process, 'exit');
      const buffer = this.process.buffer;
      delete this.process;
      return buffer;
    }
  }

  clear() {
    if (this.process != null)
      this.process.clear();
  }

  async cleanup(...args: Parameters<ChildProcess['kill']>) {
    if (this.process != null) {
      this.process.kill(...args);
      await this.waitForExit();
    }
    // automatic remove doesn't work
    this.dirs.forEach(dir => dir.removeCallback());
  }
}