// Espera o HTML da página carregar antes de executar o script
document.addEventListener("DOMContentLoaded", () => {
    
    // Pega os elementos do HTML que vamos usar
    const form = document.getElementById("filtro-form");
    const loadingDiv = document.getElementById("loading");
    const statusMessageDiv = document.getElementById("status-message");
    const resultadosContainer = document.getElementById("resultados-container");
    const buscarButton = document.getElementById("btn-buscar");

    // Adiciona um "escutador" para o evento de "submit" (clique no botão) do formulário
    form.addEventListener("submit", async (event) => {
        // Previne o recarregamento da página (comportamento padrão do formulário)
        event.preventDefault();

        // 1. Prepara a busca
        resultadosContainer.innerHTML = ""; // Limpa resultados antigos
        statusMessageDiv.innerHTML = ""; // Limpa mensagens antigas
        statusMessageDiv.className = ""; // Limpa classes de cor
        loadingDiv.classList.remove("hidden"); // Mostra "Buscando..."
        buscarButton.disabled = true; // Desabilita o botão

        // 2. Pega os dados do formulário
        const formData = new FormData(form);
        const cnpj = formData.get("cnpj");
        
        // Converte as datas do formato 'YYYY-MM-DD' para 'YYYYMMDD' (que nossa API espera)
        const dataInicio = formData.get("data_inicio").split("-").join("");
        const dataFim = formData.get("data_fim").split("-").join("");

        // 3. Monta a URL da API
        const apiUrl = `/api/gerar-relatorio?cnpj=${cnpj}&inicio=${dataInicio}&fim=${dataFim}`;

        // 4. Chama a API (fetch)
        try {
            const response = await fetch(apiUrl);

            if (!response.ok) {
                // Se a API retornar um erro (ex: 500, 400)
                const errorData = await response.json();
                throw new Error(errorData.erro || `Erro ${response.status} ao buscar dados.`);
            }

            const itens = await response.json();

            // 5. Processa os resultados
            if (itens.length === 0) {
                mostrarMensagem("Nenhum item encontrado para este período.", "status-success");
            } else {
                exibirResultados(itens);
            }

        } catch (error) {
            console.error("Erro no fetch:", error);
            mostrarMensagem(`Erro: ${error.message}`, "status-error");
        } finally {
            // 6. Finaliza a busca
            loadingDiv.classList.add("hidden"); // Esconde "Buscando..."
            buscarButton.disabled = false; // Reabilita o botão
        }
    });

    /**
     * Exibe a tabela de resultados no HTML
     * @param {Array} itens - A lista de itens vinda da API
     */
    function exibirResultados(itens) {
        let html = `
            <table>
                <thead>
                    <tr>
                        <th>Descrição do Item</th>
                        <th>Qtd.</th>
                        <th>Valor Unitário</th>
                        <th>Modalidade</th>
                        <th>Link (Edital)</th>
                    </tr>
                </thead>
                <tbody>
        `;

        const keywords = ["TERMO DE REFERÊNCIA", "ANEXO", "EDITAL", "LOTE"];

        for (const item of itens) {
            
            // --- CORREÇÃO AQUI ---
            // Constroi a URL no formato correto: {cnpj}/{ano}/{sequencial}
            const pncpLink = `https://pncp.gov.br/app/editais/${item.cnpj}/${item.ano}/${item.sequencial}`;
            // --- FIM DA CORREÇÃO ---
            
            let descricao = item.descricao || 'N/D';
            let linhaClass = ""; 

            const valorUnit = item.valor_unit_estimado || 0;
            let isLote = keywords.some(key => descricao.toUpperCase().includes(key));

            if (isLote || (valorUnit === 0 && item.quantidade === 1)) {
                linhaClass = "linha-aviso"; 
                descricao = `⚠️ <strong>${descricao}</strong><br><small>(Itens provavelmente detalhados no Termo de Referência. Clique no link ao lado para ver os anexos no PNCP.)</small>`;
            }

            html += `
                <tr class="${linhaClass}">
                    <td>${descricao}</td>
                    <td>${item.quantidade || 'N/D'}</td>
                    <td>${formatarMoeda(item.valor_unit_estimado)}</td>
                    <td>${item.licitacao_modalidade || 'N/D'}</td>
                    <td>
                        <a href="${pncpLink}" target="_blank" class="link-pncp">Ver Edital</a>
                    </td>
                </tr>
            `;
        }

        html += `</tbody></table>`;
        resultadosContainer.innerHTML = html;
    }

    /**
     * Formata um número como moeda brasileira (BRL)
     * @param {number} valor - O número a ser formatado
     */
    function formatarMoeda(valor) {
        if (valor === null || valor === undefined) {
            return "N/D";
        }
        return valor.toLocaleString("pt-BR", {
            style: "currency",
            currency: "BRL"
        });
    }

    /**
     * Mostra uma mensagem de status (erro ou sucesso)
     * @param {string} mensagem - O texto a ser exibido
     * @param {string} tipo - A classe CSS (status-error ou status-success)
     */
    function mostrarMensagem(mensagem, tipo) {
        statusMessageDiv.innerHTML = mensagem;
        statusMessageDiv.className = tipo;
    }

});