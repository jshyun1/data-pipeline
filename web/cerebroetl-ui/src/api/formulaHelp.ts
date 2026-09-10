import { apiClient, unwrap, type ApiResponse } from "./client";

export interface FormulaHelpExample {
  formula: string;
  description: string;
  result: string;
}

export interface FormulaHelpItem {
  id: number;
  functionName: string;
  category: string;
  syntax: string;
  summary: string;
  usageText: string;
  examples: FormulaHelpExample[];
  notice?: string | null;
}

export async function listFormulaHelp(): Promise<FormulaHelpItem[]> {
  const res = await apiClient.get<ApiResponse<FormulaHelpItem[]>>("/etl/formula-help");
  return unwrap(res.data);
}

export interface CalciteSqlValidationResponse {
  success: boolean;
  testedAt: string;
  latencyMs: number;
  message: string;
}

export async function validateCalciteSql(sql: string): Promise<CalciteSqlValidationResponse> {
  const res = await apiClient.post<ApiResponse<CalciteSqlValidationResponse>>("/etl/formula-help/validate", { sql });
  return unwrap(res.data);
}
