/**
 * @fileoverview 013_ConsultoriaIA - v5.0 (DUD Edition & Middleman)
 * OBJETIVO: Middleware entre o Front-end e o Motor Gemini. 
 * AÇÃO: Formata dados, aplica a Persona (via ConfigManager) e roteia o JSON.
 */

const ConsultoriaIA = {
  _serviceName: "ConsultoriaIA_v5.0",

  /**
   * Ponto de entrada chamado pelo Front-end (Web App)
   * @param {Array} operacoes - Array de objetos representando a carteira na tela.
   */
  analisarCarteira(operacoes) {
    if (!operacoes || operacoes.length === 0) {
      SysLogger.log(this._serviceName, "AVISO", "Nenhuma operação recebida do front-end.");
      SysLogger.flush();
      return { success: false, error: "Nenhuma operação recebida para análise." };
    }

    SysLogger.log(this._serviceName, "INFO", `Montando Super-Prompt para ${operacoes.length} ativos.`);

    try {
      // Função segura para buscar dados no objeto (Case Insensitive e Aceita Multi-rótulos)
      const getSafe = (obj, chavesAceitas) => {
        if (!obj) return "N/D";
        const keysObj = Object.keys(obj).map(k => k.toUpperCase());
        for (const chaveDesejada of chavesAceitas) {
          const c = chaveDesejada.toUpperCase();
          const objKeyOriginal = Object.keys(obj).find(k => k.toUpperCase() === c);
          if (objKeyOriginal && obj[objKeyOriginal] !== "" && obj[objKeyOriginal] !== null) {
            return obj[objKeyOriginal];
          }
        }
        return "N/D";
      };

      // 1. LIMPEZA E OTIMIZAÇÃO DE TOKENS (DUD Aligned)
      const carteiraLimpa = operacoes.map((op, index) => {
        return {
          _id_temp: index, // OBRIGATÓRIO PARA O MAPEAMENTO DA IA DE VOLTA AO FRONT
          Ativo: getSafe(op, ["OPTION_TICKER", "TICKER", "Código"]),
          Acao: getSafe(op, ["TICKER", "Ativo_Objeto"]),
          Side: getSafe(op, ["SIDE", "Venda/Compra"]),
          Dias_Venc: getSafe(op, ["DTE", "DTE_CALENDAR", "Vencimento_Dias"]),
          Moneyness: getSafe(op, ["MONEYNESS", "Moneyness_Code"]),
          Lucro_Pct: getSafe(op, ["PL_PCT", "P/L TOTAL %", "Lucro_Atual"]),
          Delta: getSafe(op, ["DELTA"]),
          Tendencia: getSafe(op, ["TREND", "Tendencia", "Veredito_Tendencia"])
        };
      });

      // 2. A PERSONA DINÂMICA (Lida via ConfigManager v5.0 do arquivo 001)
      const configs = ConfigManager.get();
      
      // Descobre o perfil ou cai pro padrão
      const perfilAtivo = String(configs["IA_PERFIL_CONSULTOR"] || "EQUILIBRADO").trim().toUpperCase();
      const regrasGerais = configs["PROMPT_REGRAS_GERAIS"] || "Atue como um Gestor de Risco frio e calculista.";
      const promptPerfil = configs[`PROMPT_SISTEMA_${perfilAtivo}`] || "Foque em gestão de risco.";

      // 3. O CÉREBRO DA IA (System Instruction)
      const systemInstruction = `
${regrasGerais}

${promptPerfil}

REGRA DE ISOLAMENTO E ESTRUTURAÇÃO:
1. PROIBIDO AGRUPAR: Avalie cada "_id_temp" de forma 100% isolada.
2. ESTRUTURA OBRIGATÓRIA DA ANÁLISE: Para cada ativo, você deve obrigatoriamente seguir este modelo de texto (Use Markdown básico para negrito):
   - **O QUE**: [Ação clara: MANTER, RECOMPRAR, ROLAR ou ASSUMIR]
   - **QUANDO**: [Timing exato: AGORA, PRÓXIMOS DIAS ou NO VENCIMENTO]
   - **POR QUE**: [Racional técnico denso citando Delta, Lucro Pct, Tendência ou DTE]

3. FORMATO DE SAÍDA: Retorne estritamente um array de objetos JSON mapeando o "id" ao "_id_temp" e "analise" contendo a estrutura acima.
   Exemplo: [{"id": 0, "analise": "- **O QUE**: RECOMPRAR\\n- **QUANDO**: AGORA\\n- **POR QUE**: Lucro de 94% atingido..."}]
      `.trim();

      // 4. O PROMPT DO USUÁRIO (O que a IA vai ler)
      const promptUser = `
Audite a seguinte carteira de opções e retorne o JSON mapeado pelo "_id_temp".
Carteira Atual:
${JSON.stringify(carteiraLimpa, null, 2)}
      `.trim();

      // 5. CHAMADA AO MOTOR GEMINI (012_CoreServiceIA)
      SysLogger.log(this._serviceName, "INFO", `Disparando Gemini. Perfil: ${perfilAtivo}. Qtd Itens: ${carteiraLimpa.length}`);
      
      const t0 = Date.now();
      const respostaIA = GeminiService.generate(promptUser, systemInstruction, true);
      const t1 = Date.now();

      if (!respostaIA || !Array.isArray(respostaIA)) {
        throw new Error("O motor Gemini não retornou o Array JSON esperado.");
      }

      // 6. SUCESSO E RETORNO PARA O FRONT-END
      SysLogger.log(this._serviceName, "SUCESSO", `Consultoria gerada em ${(t1-t0)/1000}s.`, `${respostaIA.length} análises retornadas.`);
      SysLogger.flush(); 
      
      return { 
        success: true, 
        data: respostaIA 
      };

    } catch (e) {
      SysLogger.log(this._serviceName, "CRITICO", "Falha na geração da consultoria IA.", String(e.message));
      SysLogger.flush();
      return { success: false, error: String(e.message) };
    }
  }
};

// ============================================================================
// PONTO DE ENTRADA DO WEB APP (Comunicação com o JS do Front)
// ============================================================================
function apiAnalisarOperacoesAtivas(operacoesJson) {
  // Se o Front mandar string, converte. Se mandar objeto, usa direto.
  let ops = typeof operacoesJson === 'string' ? JSON.parse(operacoesJson) : operacoesJson;
  return ConsultoriaIA.analisarCarteira(ops);
}

// ============================================================================
// SUÍTE DE HOMOLOGAÇÃO (Teste Unitário Sem Front-end)
// ============================================================================

/**
 * Roda um Mock (Simulação) de como o Front-end envia os dados para testar toda a cadeia.
 */
function testSuiteConsultoriaIA013() {
  console.log("=== INICIANDO TESTE UNITÁRIO: CONSULTORIA IA (013) ===");

  // MOCK: Simulando o pacote JSON que o Front-end enviaria
  const mockFrontEndData = [
    {
      OPTION_TICKER: "PETRC425",
      TICKER: "PETR4",
      SIDE: "V",
      DTE: 12,
      MONEYNESS: "OTM",
      PL_PCT: "92%",
      DELTA: "-0.15",
      TREND: "ALTA"
    },
    {
      OPTION_TICKER: "VALEP650",
      TICKER: "VALE3",
      SIDE: "V",
      DTE: 3,
      MONEYNESS: "ITM",
      PL_PCT: "-45%",
      DELTA: "-0.85",
      TREND: "BAIXA"
    }
  ];

  console.log("1. Simulando envio de dados do Front-end (2 Operações)...");
  
  const resultado = apiAnalisarOperacoesAtivas(mockFrontEndData);

  if (resultado.success) {
    console.log(`✅ SUCESSO: A IA processou e devolveu um Array com ${resultado.data.length} itens.`);
    console.log("\n--- AMOSTRA DA ANÁLISE (ITEM 0) ---");
    console.log(`ID Temporário: ${resultado.data[0].id}`);
    console.log(`Texto IA:\n${resultado.data[0].analise}`);
    console.log("-----------------------------------");
  } else {
    console.error(`❌ FALHA NO MIDDLEWARE: ${resultado.error}`);
  }

  console.log("\n⚠️ Verifique a aba 'LOGS' para checar o registro da operação.");
  console.log("=== FIM DO TESTE ===");
}