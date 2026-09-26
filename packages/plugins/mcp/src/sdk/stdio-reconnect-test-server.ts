// Small stdio MCP fixture for connection-pool recovery tests. Each process
// appends its PID so the test can verify that a replacement server was spawned.

import { appendFileSync } from "node:fs";

const pidFile = process.argv[2];
if (pidFile === undefined) {
  process.stderr.write("usage: stdio-reconnect-test-server.ts <pid-file>\n");
  process.exit(2);
}
appendFileSync(pidFile, `${process.pid}\n`);

const respond = (message: object): void => {
  process.stdout.write(`${JSON.stringify(message)}\n`);
};

const handle = (line: string): void => {
  if (!line.trim()) return;
  let request: {
    readonly id?: number;
    readonly method?: string;
    readonly params?: {
      readonly protocolVersion?: string;
      readonly name?: string;
      readonly arguments?: { readonly value?: string };
    };
  };
  // oxlint-disable-next-line executor/no-try-catch-or-throw -- boundary: standalone fixture process must ignore malformed frames
  try {
    // oxlint-disable-next-line executor/no-json-parse -- boundary: this fixture controls its JSON-RPC framing directly
    request = JSON.parse(line);
  } catch {
    return;
  }

  if (request.method === "initialize") {
    respond({
      jsonrpc: "2.0",
      id: request.id,
      result: {
        protocolVersion: request.params?.protocolVersion,
        capabilities: { tools: {} },
        serverInfo: { name: "stdio-reconnect-test-server", version: "0.0.0" },
      },
    });
  } else if (request.method === "tools/list") {
    respond({
      jsonrpc: "2.0",
      id: request.id,
      result: {
        tools: [
          {
            name: "echo",
            description: "Echo a value",
            inputSchema: {
              type: "object",
              properties: { value: { type: "string" } },
              required: ["value"],
            },
          },
        ],
      },
    });
  } else if (request.method === "tools/call") {
    respond({
      jsonrpc: "2.0",
      id: request.id,
      result: {
        content: [{ type: "text", text: request.params?.arguments?.value ?? "missing" }],
      },
    });
  } else if (request.id !== undefined) {
    respond({ jsonrpc: "2.0", id: request.id, result: {} });
  }
};

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let newline = buffer.indexOf("\n");
  while (newline >= 0) {
    handle(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\n");
  }
});
process.stdin.on("end", () => process.exit(0));
