/**
 * @fileoverview CoreServiceUI - v4.0 (Silent Mode)
 * RESPONSABILIDADE: Gerenciar a ponte de execução mantendo o silêncio total na interface.
 * PADRÃO: Zero Toasts e Zero Modais. Tudo flui em background (Console e SysLogger).
 */

const UIHandler = {
  
  /**
   * Forçamos o sistema a sempre agir como backend, 
   * ignorando qualquer tentativa de renderizar pop-ups na tela.
   */
  isBackend() {
    return true; 
  },

  /**
   * Notificação Silenciosa.
   */
  notify(mensagem, titulo = "Sistema") {
    console.info(`[NOTIFY_SILENCIADO] ${titulo}: ${mensagem}`);
  },

  /**
   * Alerta Silencioso. Redirecionado para o Console de Erros.
   */
  alert(titulo, mensagem) {
    console.warn(`[ALERT_SILENCIADO] ${titulo}: ${mensagem}`);
  }
};

// ═══════════════════════════════════════════════════════════════
// PONTES DE EXECUÇÃO (BRIDGES)
// ═══════════════════════════════════════════════════════════════

/**
 * Bridge mestre que encapsula a execução garantindo logs e silêncio.
 */
function _menuBridge(servicoNome, callback) {
  const inicio = Date.now();
  
  try {
    // Executa a função real silenciosamente
    callback();
    
    const duracao = ((Date.now() - inicio) / 1000).toFixed(1);
    console.info(`[BRIDGE] ${servicoNome} concluído com sucesso em ${duracao}s.`);
    
    // Garante que os logs da execução sejam salvos na aba Logs
    if (typeof SysLogger !== 'undefined') SysLogger.flush();

  } catch (e) {
    console.error(`[BRIDGE_ERRO] Falha em ${servicoNome}: ${e.message}`);
    
    if (typeof SysLogger !== 'undefined') {
      // Uso de String() para garantir a proteção da primeira coluna do 003
      SysLogger.log("UI_BRIDGE", "ERRO", `Falha fatal em ${servicoNome}`, String(e.message));
      SysLogger.flush();
    }
  }
}

// ═══════════════════════════════════════════════════════════════
// MAPA DE FUNÇÕES DO MENU
// ═══════════════════════════════════════════════════════════════

function AtualizarNecton_Menu()      { _menuBridge("Necton", atualizarNecton); }
function AtualizarDadosAtivos_Menu() { _menuBridge("Ativos", atualizarDadosAtivos); }
function AtualizarHistorico_Menu()   { _menuBridge("Histórico", atualizarDadosHistoricos); }
function AtualizarDetalhes_Menu()    { _menuBridge("Detalhes", atualizarDetalhesOpcoes); }
function AtualizarGregasAPI_Menu()   { _menuBridge("Gregas (API)", atualizarGregas); }
function CalcularGregasNativo_Menu() { _menuBridge("Gregas (Nativo)", calcularGregasNativo); }
function AtualizarScanner_Menu()     { _menuBridge("Scanner Oportunidades", atualizarScannerOpcoes); }

// ═══════════════════════════════════════════════════════════════
// TESTE DE HOMOLOGAÇÃO (004)
// ═══════════════════════════════════════════════════════════════

function testSuiteUIHandler() {
  console.log("=== HOMOLOGANDO INTERFACE v4.0 (SILENT MODE) ===");
  
  UIHandler.notify("Isso não deve aparecer na tela.", "Teste");
  UIHandler.alert("Isso também não deve aparecer na tela.", "Teste de Erro");
  
  // Teste da Bridge com erro simulado
  console.log("Testando resiliência da Bridge silenciosa...");
  _menuBridge("Teste_Silencioso", () => {
    throw new Error("Simulação de falha para validar apenas o Log interno.");
  });

  console.log("=== FIM DA HOMOLOGAÇÃO 004 ===");
}