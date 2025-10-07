import * as restate from "@restatedev/restate-sdk";
import { durableCalls } from "@restatedev/vercel-ai-middleware";
import { openai } from "@ai-sdk/openai";
import { generateText, tool, wrapLanguageModel, stepCountIs } from "ai";
import { z } from "zod";

export const InsuranceClaimSchema = z.object({
  date: z.string().nullable().optional(),
  category: z.string().nullable().optional(),
  reason: z.string().nullable().optional(),
  amount: z.number().nullable().optional(),
  placeOfService: z.string().nullable().optional(),
});

export type InsuranceClaim = z.infer<typeof InsuranceClaimSchema>;

export const claimApprovalAgentWithHumanApproval = restate.workflow({
  name: "ClaimApprovalAgent",
  handlers: {
    run: async (ctx: restate.WorkflowContext, { customerId, amount} : { customerId: string; amount: number }) => {
      
      // enrich with database data
      const customerPolicy = await ctx.run("fetch customer policy from DB", () =>
        retrieveCustomerPolicy(customerId),
      );
      
      const model = wrapLanguageModel({
        model: openai("gpt-4o"),
        middleware: durableCalls(ctx),
      });

      const { text } = await generateText({
        model,
        system:
          "You are an insurance claim evaluation agent. Use these rules:\n " +
          "* if the amount is more than 1000, ask for human approval,\n" +
          "* if the amount is less than 1000, decide by yourself\n" +
          "* you only need the amount and no further details about the claim",
        prompt: `Evaluate the claim for ${amount}\n\nCustomer Policy Info: ${JSON.stringify(
          customerPolicy
        )}`,
        tools: {
          humanApproval: tool({
            description: "Ask for human approval for high-value claims.",
            inputSchema: InsuranceClaimSchema,
            execute: async (claim: InsuranceClaim) => {
              await ctx.run("request human review", () =>
                notifyHumanReviewer(claim, ctx.key)
              );

              return await ctx.promise<boolean>("approval");
            },
          }),
        },
        stopWhen: [stepCountIs(5)],
      });

      return { response: text };
    },

    onHumanApprovalReady: async (
      ctx: restate.WorkflowSharedContext,
      approval: boolean
    ) => {
      ctx.promise("approval").resolve(approval);
    },
  },
});


// UTILS

async function notifyHumanReviewer(message: InsuranceClaim, key: string) {
  await new Promise((r) => setTimeout(r, 2000));
  console.log(`>>> ${message} \n`);
}

export function retrieveCustomerPolicy(customerId: string) {
  console.log(`Retrieving policy info for customer ${customerId}...`);
  return {
    policyNumber: "POL123456",
    coverage: "Full",
    validTill: "2025-12-31",
  };
}