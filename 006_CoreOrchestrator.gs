/**
 * @fileoverview CoreOrchestrator - v4.2 (The Brain - Gold Standard)
 * RESPONSABILIDADE: Gerenciar a execução, validar ambiente e impor a ordem lógica.
 * INTEGRAÇÃO: Orquestra os Motores usando a Infraestrutura (000-004).
 */

const CoreOrchestrator = {
  _serviceName: "CoreOrchestrator",

  /**
   * Registro Central de Serviços.
   * Centraliza onde cada motor de cálculo está localizado e seus nomes limpos.
   */
  get REGISTRY() {
    return {
      "ATUALIZAR_NECTON": {
        nome: "Atualizar Necton (Portfólio)",
        exec: () => typeof PortfolioUpdater !== 'undefined' ? PortfolioUpdater.syncPortfolioData() : console.warn("Motor 006 não carregado."),
        requer_token: true
      },
      "ATUALIZAR_ATIVOS": {
        nome: "Atualizar Dados Ativos",
        exec: () => typeof StockDataSync !== 'undefined' ? StockDataSync.run() : console.warn("Motor 007 não carregado."),
        requer_token: true
      }
    };
  },

  /**
   * Checklist de Voo: Verifica se o sistema pode operar antes de rodar qualquer motor.
   */
  validarAmbiente() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const erros = [];

    // 1. Verifica abas essenciais
    [SYS_CONFIG.SHEETS.IMPORT, SYS_CONFIG.SHEETS.LOGS, SYS_CONFIG.SHEETS.CONFIG].forEach(aba => {
    if (!ss.getSheetByName(aba)) erros.push(`Aba '${aba}' ausente.`);
    });

    // 2. Verifica Token da API
    const token = PropertiesService.getScriptProperties().getProperty("OPLAB_ACCESS_TOKEN");
    if (!token) erros.push("Token OPLAB ausente no PropertiesService.");

    if (erros.length > 0) {
      const msg = "🚨 Falha de Ambiente:\n" + erros.join("\n");
      UIHandler.alert("Erro de Configuração", msg);
      
      // Uso de JSON.stringify para proteger a coluna de Timestamp no 003
      SysLogger.log(this._serviceName, "CRITICO", "Ambiente inválido para execução.", JSON.stringify({ falhas: erros }));
      return false;
    }
    return true;
  },


/**
   * Lê a aba Config_Global e extrai a sequência de funções a serem executadas.
   */
  getSequenciaDinamica() {
    try {
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const abaConfig = ss.getSheetByName("Config_Global");
      
      if (!abaConfig) {
        throw new Error("Aba 'Config_Global' não encontrada.");
      }

      // Lê as duas primeiras colunas (Chave e Valor) da aba de configuração
      const data = abaConfig.getDataRange().getValues();
      
      for (let i = 0; i < data.length; i++) {
        if (String(data[i][0]).trim() === "Orquestrador_Sequencia_Padrao") {
          const sequenciaRaw = String(data[i][1]).trim();
          
          if (!sequenciaRaw) return [];
          
          // Divide pelo ponto-e-vírgula e remove espaços extras
          return sequenciaRaw.split(';').map(f => f.trim()).filter(f => f.length > 0);
        }
      }
      return []; // Se não achar a chave, retorna vazio
    } catch (e) {
      SysLogger.log(this._serviceName, "ERRO", "Falha ao ler Config_Global", String(e.message));
      return [];
    }
  },



  /**
   * Executa um serviço individual.
   * @return {boolean} true se sucesso, false se falha.
   */
  executarServico(chave) {
    const servico = this.REGISTRY[chave];
    if (!servico) {
      SysLogger.log(this._serviceName, "ERRO", `Serviço '${chave}' não encontrado no Registro.`, "");
      return false;
    }

    if (!this.validarAmbiente()) return false;

    try {
      SysLogger.log(this._serviceName, "INFO", `Delegando execução para: ${servico.nome}`, "");
      servico.exec();
      SysLogger.flush();
      return true;
    } catch (e) {
      // e.message garante que enviaremos uma String ao Logger
      SysLogger.log(this._serviceName, "ERRO", `Falha catastrófica no motor: ${servico.nome}`, String(e.message));
      SysLogger.flush();
      UIHandler.notify(`Falha em ${servico.nome}`, "Erro ❌");
      return false;
    }
  },

  /**
   * FLUXO MESTRE: Executa a sequência completa com trava de segurança (Curto-Circuito).
   */
/**
   * FLUXO MESTRE DINÂMICO: Lê a configuração da planilha e executa a sequência.
   */
    runFluxoMestre() {
    const sequencia = this.getSequenciaDinamica();
    
    if (sequencia.length === 0) {
      UIHandler.alert("Orquestrador", "Nenhuma sequência definida na chave 'Orquestrador_Sequencia_Padrao' da aba Config_Global.");
      return;
    }

    // MARCADOR DE TERRITÓRIO: INÍCIO DO FLUXO
    SysLogger.log(this._serviceName, "START", ">>> INICIANDO FLUXO MESTRE DINÂMICO <<<", JSON.stringify({ sequencia_encontrada: sequencia }));

    // Captura o ambiente global do Apps Script para conseguir chamar funções pelo nome em texto
    const contextoGlobal = (function() { return this; })();

    for (let i = 0; i < sequencia.length; i++) {
      const nomeFuncao = sequencia[i];
      // Exibe no canto da tela: "Passo 1/2: atualizarNecton..."
      UIHandler.notify(`Passo ${i+1}/${sequencia.length}: Executando ${nomeFuncao}...`, "Orquestrador");
      
      try {
        const funcaoAlvo = contextoGlobal[nomeFuncao];
        
        // Verifica se o texto digitado na planilha realmente é o nome de uma função no código
        if (typeof funcaoAlvo === 'function') {
          
          SysLogger.log(this._serviceName, "INFO", `Invocando passo: ${nomeFuncao}`, "");
          funcaoAlvo(); // Executa a função magicamente aqui!
          SysLogger.flush();
          
        } else {
          throw new Error(`A função '${nomeFuncao}' não existe no código. Verifique a ortografia na planilha.`);
        }
        
      } catch (e) {
        SysLogger.log(this._serviceName, "ERRO", `Fluxo interrompido no passo: ${nomeFuncao}`, String(e.message));
        SysLogger.flush();
        UIHandler.alert("Fluxo Interrompido", `Falha ao executar "${nomeFuncao}".\n\nErro: ${e.message}\n\nO processo foi parado por segurança.`);
        return; // Curto-circuito: para tudo!
      }
    }

    // MARCADOR DE TERRITÓRIO: FINALIZAÇÃO
    SysLogger.log(this._serviceName, "FINISH", ">>> FLUXO MESTRE CONCLUÍDO COM SUCESSO <<<", JSON.stringify({ total_passos: sequencia.length }));
    SysLogger.flush();
    UIHandler.alert("Fluxo Concluído", `Sincronização global concluída!\nPassos executados: ${sequencia.join(", ")}`);
  }
};

// ============================================================================
// PONTES DE COMPATIBILIDADE E FLUXO MESTRE
// ============================================================================

/** Função acionada pelo menu para rodar tudo na sequência correta */
function executarFluxoSequencial() { 
  CoreOrchestrator.runFluxoMestre(); 
}

// ============================================================================
// TESTES UNITÁRIOS DINÂMICOS (005)
// ============================================================================

/**
 * @fileoverview TestSuiteOrchestrator - v5.0
 * Homologação do Fluxo Mestre Dinâmico.
 */
function testSuiteOrchestrator() {
  console.log("=== INICIANDO HOMOLOGAÇÃO DO ORQUESTRADOR (005) ===");

  // 1. TESTE DE AMBIENTE
  console.log("--- 1. Validando Checklist de Ambiente ---");
  const ambienteOk = CoreOrchestrator.validarAmbiente();
  console.log(`Status do Ambiente: ${ambienteOk ? "PRONTO ✅" : "PROBLEMAS DETECTADOS ⚠️"}`);

  // 2. TESTE DE LEITURA DINÂMICA (Aba Config_Global)
  console.log("--- 2. Lendo Sequência da Config_Global ---");
  const seq = CoreOrchestrator.getSequenciaDinamica();
  if (seq.length > 0) {
    console.log(`✅ Sucesso! Passos encontrados: ${seq.join(" ➡️ ")}`);
  } else {
    console.error("❌ Falha: Nenhuma sequência encontrada. Preencha 'Orquestrador_Sequencia_Padrao'.");
  }

  // 3. VALIDAÇÃO DE EXISTÊNCIA DAS FUNÇÕES NO CÓDIGO
  console.log("--- 3. Verificando se os passos apontam para funções reais ---");
  const contextoGlobal = (function() { return this; })();
  
  if (seq.length === 0) console.log("Nenhuma função para testar.");
  
  seq.forEach(nome => {
    const existe = typeof contextoGlobal[nome] === 'function';
    console.log(`Função ${nome}(): ${existe ? "EXISTE NO CÓDIGO ✅" : "NÃO ENCONTRADA (Verifique a ortografia) ❌"}`);
  });

  console.log("=== FIM DA HOMOLOGAÇÃO 005 ===");
}