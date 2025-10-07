import * as restate from "@restatedev/restate-sdk/fetch";
import { claimApprovalAgentWithHumanApproval } from "@/restate/greeter2";

const endpoint = restate.createEndpointHandler({
  services: [claimApprovalAgentWithHumanApproval],
  identityKeys: ["publickeyv1_A25Cm7CqPJqoHUj8KrvSGrs6g5wE1TGY2HMBVedFd2s5"],
});

// Adapt it to Next.js route handlers
export const serve = () => {
  return {
    POST: (req: Request) => endpoint(req),
    GET: (req: Request) => endpoint(req),
  };
};
