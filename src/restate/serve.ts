import * as restate from "@restatedev/restate-sdk/fetch";
import { agent } from "@/restate/agent";
import { agentExecutor } from "@/restate/agent_executor";
import {modal} from "@/restate/modal";
import { sandbox } from "@/restate/sandbox";

const endpoint = restate.createEndpointHandler({
  services: [agent, agentExecutor, modal, sandbox],
  identityKeys: ["publickeyv1_A25Cm7CqPJqoHUj8KrvSGrs6g5wE1TGY2HMBVedFd2s5"],
});

// Adapt it to Next.js route handlers
export const serve = () => {
  return {
    POST: (req: Request) => endpoint(req),
    GET: (req: Request) => endpoint(req),
  };
};
