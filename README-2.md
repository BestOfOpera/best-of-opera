# 🎭 Best of Opera - APP1 Curadoria v3 (SQLite Cache)

## 🚀 O QUE MUDOU (Versão 3.0)

### ✅ Implementado:

1. **Cache SQLite** - Armazena resultados de buscas por categoria
2. **População Automática** - Cache é preenchido automaticamente no primeiro acesso
3. **Playlist Pré-Aprovados** - Botão destacado no topo com vídeos curados
4. **Busca Otimizada** - Busca por termo limitada a 10 resultados (economiza quota)
5. **Indicadores Visuais** - Mostra status do cache e última atualização
6. **Endpoints de Refresh** - Atualizar cache e playlist manualmente

---

## 📊 ECONOMIA DE QUOTA

### Antes (v2):
- 7 categorias × 11 queries = **77 buscas** = **7.700 pontos**
- Restava: **2.300 pontos** (~23 buscas/dia)

### Agora (v3):
- Cache populacional: **1x por mês** (ou manual)
- Playlist: **1x por semana** (automática)
- **Sobram ~100 buscas por termo/dia!** 🎉

---

## 📁 ESTRUTURA DE ARQUIVOS

```
best-of-opera/
├── main.py              # Backend FastAPI (ATUALIZADO)
├── database.py          # Módulo SQLite (NOVO)
├── requirements.txt     # Dependências
├── static/
│   └── index.html       # Frontend React (ATUALIZADO)
└── data/
    ├── cache.db         # SQLite database (criado automaticamente)
    └── dataset_v3_categorizado.csv  # Posted registry
```

---

## 🔧 DEPLOY NO RAILWAY

### 1. **Substituir arquivos no GitHub:**

```bash
# Clone o repo (se ainda não tiver)
git clone https://github.com/BestOfOpera/best-of-opera.git
cd best-of-opera

# Copie os 3 arquivos atualizados para o repo:
# - main.py (ATUALIZADO)
# - database.py (NOVO - criar na raiz)
# - static/index.html (ATUALIZADO)

# Commit e push
git add .
git commit -m "✨ v3.0: SQLite cache + Playlist pré-aprovados + Economia de quota"
git push origin main
```

### 2. **Railway vai detectar e fazer redeploy automático**

✅ O Railway já está configurado corretamente!  
✅ SQLite será criado em `/app/data/cache.db`  
✅ Cache será populado automaticamente no primeiro acesso

---

## 🎯 COMO USAR

### **1. Primeira Vez:**
- Abra: `https://web-production-854ed.up.railway.app`
- O sistema vai popular o cache automaticamente (background)
- Aguarde 2-3 minutos e recarregue a página

### **2. Busca por Categoria:**
- Clique em qualquer categoria (ex: "Grandes Nomes")
- **Primeira vez**: busca no YouTube + salva no cache
- **Próximas vezes**: retorna do cache instantaneamente! ⚡

### **3. Playlist Pré-Aprovados:**
- Clique no botão **"📝 PLAYLIST PRÉ-APROVADOS"** no topo
- Mostra vídeos já curados da playlist do YouTube
- Atualização automática: **1x por semana**

### **4. Busca por Termo (Live):**
- Digite termo livre (ex: "vivaldi")
- Limitado a **10 resultados** (economiza quota)
- Ainda usa YouTube API diretamente

### **5. Atualizar Cache (Mensal):**
- Clique em **"🔄 Atualizar Cache"**
- Confirme e aguarde alguns minutos
- Recomendado: **1x por mês**

---

## 📡 NOVOS ENDPOINTS DA API

### Cache:
```
GET  /api/cache/status              # Ver status do cache
POST /api/cache/populate-initial    # Popular cache (manual)
POST /api/cache/refresh-categories  # Atualizar todas categorias
```

### Playlist:
```
GET  /api/playlist/videos          # Listar vídeos da playlist
POST /api/playlist/refresh         # Atualizar playlist do YouTube
```

### Categorias (modificado):
```
GET /api/category/{category}?force_refresh=false
# Por padrão usa cache, force_refresh=true busca novamente
```

### Busca (modificado):
```
GET /api/search?q=termo&max_results=10
# Agora limitado a 10 resultados por padrão
```

---

## ⚙️ VARIÁVEIS DE AMBIENTE (Railway)

Já configuradas:
```
YOUTUBE_API_KEY=sua_chave_aqui
DATASET_PATH=./data/dataset_v3_categorizado.csv
STATIC_PATH=./static
```

---

## 🔄 FLUXO DE AUTOMAÇÃO

### **No Startup:**
1. Inicializa database SQLite
2. Carrega Posted Registry
3. Verifica se cache está vazio
4. Se vazio → inicia população automática (background)

### **Na Busca por Categoria:**
1. Tenta buscar do cache primeiro
2. Se não existe → busca YouTube + salva cache
3. Retorna resultados ranqueados

### **Playlist:**
1. Cache atualizado manualmente ou por cronjob
2. Resultados sempre do cache (instantâneo)

---

## 📌 LEMBRETES

### **Mensal:**
- [ ] Atualizar cache de categorias (botão 🔄)
- [ ] Upload arquivo Meta Analytics (últimos 30 dias)
- [ ] Revisar Posted Registry

### **Semanal:**
- [ ] Refresh playlist (automático ou manual)

---

## 🐛 TROUBLESHOOTING

### Cache não atualiza:
```bash
# Ver logs no Railway
# Ou chamar endpoint manualmente:
curl -X POST https://web-production-854ed.up.railway.app/api/cache/refresh-categories
```

### Playlist vazia:
```bash
# Atualizar manualmente:
curl -X POST https://web-production-854ed.up.railway.app/api/playlist/refresh
```

### Quota esgotada:
- Verificar quantas buscas diretas foram feitas
- Usar mais o cache (categorias)
- Aguardar reset diário (meia-noite PST)

---

## 📈 PRÓXIMOS PASSOS (Fase 2)

- [ ] Cronjob para refresh automático semanal da playlist
- [ ] Notificações por email (lembrete mensal)
- [ ] Dashboard de analytics (quota usado, cache hit rate)
- [ ] Automação de geração de legendas
- [ ] Sistema de tradução multi-idioma

---

## 👨‍💻 DESENVOLVIDO POR

**Bolivar** - Best of Opera  
Powered by **Claude Sonnet 4.5** | **FastAPI** | **YouTube Data API v3**

---

**Versão:** 3.0.0  
**Data:** 2026-02-11  
**Status:** ✅ Production Ready
