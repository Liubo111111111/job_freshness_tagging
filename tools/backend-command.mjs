import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import process from "node:process";

const repoRoot = process.cwd();
const backendRoot = path.join(repoRoot, "backend");
const backendSrc = path.join(backendRoot, "src");

const pythonCandidates = [
  path.join(backendRoot, ".venv", "Scripts", "python.exe"),
  path.join(repoRoot, ".venv", "Scripts", "python.exe"),
  path.join(backendRoot, ".venv", "bin", "python"),
  path.join(repoRoot, ".venv", "bin", "python"),
  "python",
];

const python = pythonCandidates.find((candidate) =>
  candidate === "python" ? true : existsSync(candidate),
);

if (!python) {
  console.error("No Python interpreter found. Expected backend/.venv, .venv, or python in PATH.");
  process.exit(1);
}

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Usage: node tools/backend-command.mjs <python args...>");
  process.exit(1);
}

const child = spawn(python, args, {
  cwd: backendRoot,
  env: {
    ...process.env,
    PYTHONPATH: process.env.PYTHONPATH
      ? `${backendSrc}${path.delimiter}${process.env.PYTHONPATH}`
      : backendSrc,
  },
  stdio: "inherit",
  shell: false,
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});

child.on("error", (error) => {
  console.error(`Failed to start backend command: ${error.message}`);
  process.exit(1);
});
