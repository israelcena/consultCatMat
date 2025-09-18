const axios = require("axios");

// Estrutura da tabela
// CREATE TABLE "lowcode_apps"."app_catmatser" (
// "id" SERIAL,
// "idcatmat" bigint NULL  << = codigoItem
// "nome" varchar(300) NOT NULL,  << = nomePdm
// "descricao" text NOT NULL,     << = descricaoItem
// "tipo" varchar(50) NULL ,      << = "Permanente"
// "codigo_universal_catmat" varchar(100) NULL , << = idcatmat (códigoItem), string!
// "ativo" bpchar(1) NULL ,       << = "S"
// )

const BASE_URL = "https://dadosabertos.compras.gov.br/modulo-material/4_consultarItemMaterial";
const TAMANHO_PAGINA = 500;

// Função para criar registro no banco (aqui só exibe no console)
async function create(itemTabela) {
  // Substitua este console.log pelo seu insert real
  console.log("Vou criar no banco:", itemTabela);
}

async function importarCatmat() {
  try {
    // Descobrir total de páginas
    const primeiraUrl = `${BASE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=1`;
    const primeiraResp = await axios.get(primeiraUrl);
    const totalPaginas = primeiraResp.data.totalPaginas;
    console.log("TOTAL DE PÁGINAS:", totalPaginas);

    // Agora, para cada página...
    for (let pagina = 1; pagina <= totalPaginas; pagina++) {
      const url = `${BASE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=${pagina}`;
      console.log(`Baixando página ${pagina}/${totalPaginas}`);

      try {
        const resp = await axios.get(url);
        const itens = resp.data.resultado;

        for (const item of itens) {
          // Monta objeto conforme tabela
          const registroTabela = {
            idcatmat: item.codigoItem,
            nome: item.nomePdm,
            descricao: item.descricaoItem,
            tipo: "Permanente",
            codigo_universal_catmat: String(item.codigoItem),
            ativo: item.statusItem ? "S" : "N",
            atualizacao: item.dataHoraAtualizacao
          };

          await create(registroTabela);
        }
      } catch (errPag) {
        console.error("ERRO página " + pagina + ":", errPag.message);
        continue;
        // Se quiser continuar mesmo com erro: continue;
        // Se quiser parar: throw errPag;
      }
    }
    console.log("-- FINALIZADO --");
  } catch (err) {
    console.error("Erro inicial:", err.message);
  }
}

async function importarCatser() {
  try {
    // Descobre total de páginas
    const primeiraUrl = `${SERVICE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=1`;
    const primeiraResp = await axios.get(primeiraUrl);
    const totalPaginas = primeiraResp.data.totalPaginas;
    console.log("TOTAL DE PÁGINAS:", totalPaginas);

    // Para cada página...
    for (let pagina = 1; pagina <= totalPaginas; pagina++) {
      const url = `${SERVICE_URL}?tamanhoPagina=${TAMANHO_PAGINA}&pagina=${pagina}`;
      console.log(`Baixando página ${pagina}/${totalPaginas}`);

      try {
        const resp = await axios.get(url);
        const itens = resp.data.resultado;

        for (const item of itens) {
          // Monta objeto conforme tabela
          const registroTabela = {
            idcatmat: item.codigoServico,
            nome: item.nomeServico,
            descricao: item.nomeClasse,
            tipo: "Serviço",
            codigo_universal_catmat: String(item.codigoItem),
            ativo: item.statusServico ? "S" : "N",
            atualizacao: item.dataHoraAtualizacao
          };

          await create(registroTabela);
        }
      } catch (errPag) {
        console.error("ERRO página " + pagina + ":", errPag.message);
        continue; // Comente ou descomente conforme desejar pular a página com erro
      }
    }
    console.log("-- FINALIZADO [SERVIÇOS] --");
  } catch (err) {
    console.error("Erro inicial:", err.message);
  }
}

// --- Função para rodar ambas paralelamente
async function importarCatmatECatserParalelo() {
  await Promise.all([
    importarCatmat(),
    importarCatser()
  ]);
  console.log("AMBOS PROCESSOS FINALIZADOS");
}

// Para executar:
importarCatmatECatserParalelo();

