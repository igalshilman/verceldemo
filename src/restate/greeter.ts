import * as restate from "@restatedev/restate-sdk";
import { serde } from "@restatedev/restate-sdk-zod";
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
  name: "LoanApprovalWorkflow",
  handlers: {
    run: async (ctx: restate.WorkflowContext, amount: number) => {
      const model = wrapLanguageModel({
        model: openai("gpt-4o"),
        middleware: durableCalls(ctx),
      });

      const { text } = await generateText({
        model,
        system:
          "You are an insurance claim evaluation agent. Use these rules: " +
          "* if the amount is more than 1000, ask for human approval, " +
          "* if the amount is less than 1000, decide by yourself",
        prompt: `Please evaluate the following insurance claim: ${amount}.`,
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

    onHumanApproval: async (
      ctx: restate.WorkflowSharedContext,
      approval: boolean
    ) => {
      ctx.promise("approval").resolve(approval);
    },
  },
});

// UTILS

export function notifyHumanReviewer(message: InsuranceClaim, key: string) {
  console.log(`>>> ${message} \n`);
}
