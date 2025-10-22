// Espera o HTML da página carregar antes de executar o script
document.addEventListener("DOMContentLoaded", () => {
    
    // Pega os elementos do HTML que vamos usar
    const form = document.getElementById("filtro-form");
    const loadingDiv = document.getElementById("loading");
    const statusMessageDiv = document.getElementById("status-message");
    const resultadosContainer = document.getElementById("resultados-container");
    const buscarButton = document.getElementById("btn-buscar");

    // --- NOVO: Elementos do Modal ---
    const modal = document.getElementById("contribution-modal");
    const modalCloseBtn = document.querySelector(".modal-close-btn");
    const modalForm = document.getElementById("contribution-form");
    const modalSubmitBtn = document.getElementById("modal-submit");
    const modalItemDesc = document.getElementById("modal-item-descricao");
    const modalItemKey = document.getElementById("modal-item-key");
    const modalItemStatus = document.getElementById("modal-item-status");
    const modalLink = document.getElementById("modal-link");
    const modalComment = document.getElementById("modal-comment");

    // --- NOVO: Verifica se o usuário está logado (do 'base.html') ---
    const isUserAuthenticated = document.body.dataset.isAuthenticated === 'true';

    // Adiciona um "escutador" para o evento de "submit" (clique no botão) do formulário
    form.addEventListener("submit", async (event) => {
        // Previne o recarregamento da página
        event.preventDefault();
        await buscarResultados(); // Chama a função de busca
    });

    /**
     * Função principal que busca e exibe os resultados
     */
    async function buscarResultados() {
        // 1. Prepara a busca
        resultadosContainer.innerHTML = ""; // Limpa resultados antigos
        statusMessageDiv.innerHTML = ""; // Limpa mensagens antigas
        statusMessageDiv.className = ""; // Limpa classes de cor
        loadingDiv.classList.remove("hidden"); // Mostra "Buscando..."
        buscarButton.disabled = true; // Desabilita o botão

        // 2. Pega os dados do formulário
        const formData = new FormData(form);
        const cnpj = formData.get("cnpj");
        const dataInicio = formData.get("data_inicio").split("-").join("");
        const dataFim = formData.get("data_fim").split("-").join("");

        // 3. Monta a URL da API
        const apiUrl = `/api/gerar-relatorio?cnpj=${cnpj}&inicio=${dataInicio}&fim=${dataFim}`;

        // 4. Chama a API (fetch)
        try {
            const response = await fetch(apiUrl);
            if (!response.ok) {
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
    }


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
            const pncpLink = `https://pncp.gov.br/app/editais/${item.cnpj}/${item.ano}/${item.sequencial}`;
            let descricao = item.descricao || 'N/D';
            let linhaClass = ""; 
            const valorUnit = item.valor_unit_estimado || 0;
            let isLote = keywords.some(key => descricao.toUpperCase().includes(key));

            if (isLote || (valorUnit === 0 && item.quantidade === 1)) {
                linhaClass = "linha-aviso"; 
                descricao = `⚠️ <strong>${descricao}</strong><br><small>(Itens provavelmente detalhados no Termo de Referência. Clique no link ao lado para ver os anexos no PNCP.)</small>`;
                // (Aqui no futuro entrará a Feature B: "Detalhar Lote")
            }

            // --- NOVO: Renderiza a seção de votação ---
            const contribuicoes = item.contribuicoes || [];
            
            // Conta os votos
            const votosSobrepreco = contribuicoes.filter(c => c.status === 'SOBREPRECO').length;
            const votosPrecoOk = contribuicoes.filter(c => c.status === 'PRECO_OK').length;
            const votosAbaixoPreco = contribuicoes.filter(c => c.status === 'ABAIXO_PRECO').length;

            let voteSectionHTML = `
                <div class="vote-section">
                    <div class="vote-counts">
                        <span>📈 ${votosSobrepreco}</span>
                        <span>✅ ${votosPrecoOk}</span>
                        <span>📉 ${votosAbaixoPreco}</span>
                    </div>
            `;

            if (isUserAuthenticated) {
                voteSectionHTML += `
                    <div class="vote-buttons">
                        <button class="btn-vote" data-item-key="${item.item_key}" data-vote-status="SOBREPRECO" data-item-desc="${item.descricao.substring(0, 50)}...">📈 Acima</button>
                        <button class="btn-vote" data-item-key="${item.item_key}" data-vote-status="PRECO_OK" data-item-desc="${item.descricao.substring(0, 50)}...">✅ Na Média</button>
                        <button class="btn-vote" data-item-key="${item.item_key}" data-vote-status="ABAIXO_PRECO" data-item-desc="${item.descricao.substring(0, 50)}...">📉 Abaixo</button>
                    </div>
                `;
            }
            voteSectionHTML += '</div>';
            // --- FIM DA SEÇÃO DE VOTAÇÃO ---


            html += `
                <tr class="${linhaClass}">
                    <td>
                        ${descricao}
                        ${voteSectionHTML} </td>
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

    // --- NOVOS EVENT LISTENERS PARA O MODAL ---

    // Event Listener para abrir o modal (usando delegação de evento)
    resultadosContainer.addEventListener('click', (event) => {
        const voteButton = event.target.closest('.btn-vote');
        if (voteButton) {
            if (!isUserAuthenticated) {
                alert("Você precisa estar logado para contribuir.");
                return;
            }
            // Preenche o modal com os dados do item clicado
            modalItemDesc.textContent = voteButton.dataset.itemDesc;
            modalItemKey.value = voteButton.dataset.itemKey;
            modalItemStatus.value = voteButton.dataset.voteStatus;
            
            // Limpa o formulário e abre o modal
            modalForm.reset();
            modal.classList.remove('hidden');
        }
    });

    // Event Listener para fechar o modal (no 'X')
    modalCloseBtn.addEventListener('click', () => {
        modal.classList.add('hidden');
    });

    // Event Listener para fechar o modal (clicando fora)
    modal.addEventListener('click', (event) => {
        if (event.target === modal) {
            modal.classList.add('hidden');
        }
    });

    // Event Listener para o envio do formulário do modal
    modalForm.addEventListener('submit', async (event) => {
        event.preventDefault(); // Previne o recarregamento
        
        const link = modalLink.value;
        if (!link) {
            alert("Por favor, insira um link de referência.");
            return;
        }
        
        modalSubmitBtn.disabled = true;
        modalSubmitBtn.textContent = "Enviando...";

        const data = {
            item_key: modalItemKey.value,
            status: modalItemStatus.value,
            link: link,
            comment: modalComment.value
        };

        try {
            const response = await fetch('/api/contribuir', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.erro || "Erro ao enviar contribuição.");
            }

            // Sucesso!
            modal.classList.add('hidden');
            mostrarMensagem("Obrigado pela sua contribuição!", "status-success");
            
            // Recarrega os resultados para mostrar o novo voto
            // (O cache foi limpo no backend, então esta busca trará dados novos)
            await buscarResultados(); 

        } catch (error) {
            alert(error.message); // Mostra o erro
        } finally {
            modalSubmitBtn.disabled = false;
            modalSubmitBtn.textContent = "Enviar Contribuição";
        }
    });


    // --- FUNÇÕES UTILITÁRIAS (Sem mudança) ---

    function formatarMoeda(valor) {
        if (valor === null || valor === undefined) {
            return "N/D";
        }
        return valor.toLocaleString("pt-BR", {
            style: "currency",
            currency: "BRL"
        });
    }

    function mostrarMensagem(mensagem, tipo) {
        statusMessageDiv.innerHTML = mensagem;
        statusMessageDiv.className = tipo;
    }
 
});