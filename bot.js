require('dotenv').config();
const express    = require('express');
const { Telegraf } = require('telegraf');
const { createClient } = require('@supabase/supabase-js');
const { GoogleGenerativeAI } = require('@google/generative-ai');

// ─────────────────────────────────────────────────────────────
// 1. VALIDATE ENVIRONMENT
// ─────────────────────────────────────────────────────────────
const REQUIRED_ENV = [
  'TELEGRAM_BOT_TOKEN',
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'GEMINI_API_KEY',
];
for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`❌  Missing env variable: ${key}`);
    process.exit(1);
  }
}

// ─────────────────────────────────────────────────────────────
// 2. INITIALIZE CLIENTS
// ─────────────────────────────────────────────────────────────
const bot = new Telegraf(process.env.TELEGRAM_BOT_TOKEN);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

// gemini-pro: legacy alias, widest availability on free tier.
// No generationConfig, no systemInstruction — all persona & JSON rules go in the prompt.
const geminiModel = genAI.getGenerativeModel({ model: 'gemini-pro' });

// ─────────────────────────────────────────────────────────────
// 3. THE GEMINI SHIELD — RATE-LIMIT AWARE MESSAGE QUEUE
//
//    Free tier allows ~10–15 requests/min (≈1 req/6 s).
//    Each queue item waits QUEUE_DELAY_MS before the next
//    one begins, smoothing bursts into a controlled stream.
// ─────────────────────────────────────────────────────────────
const QUEUE_DELAY_MS = 5_000;   // 5 s between Gemini calls → safe at free tier
const MAX_QUEUE_SIZE = 50;      // drop oldest if flooded

/** @type {Array<{ ctx: import('telegraf').Context, telegramId: number, userMessage: string }>} */
const messageQueue = [];
let isProcessing   = false;

/**
 * Drain the queue one item at a time, respecting QUEUE_DELAY_MS between calls.
 */
async function processQueue() {
  if (isProcessing || messageQueue.length === 0) return;
  isProcessing = true;

  const { ctx, telegramId, userMessage } = messageQueue.shift();

  try {
    // ── Step A: Security check — fetch user from DB ──────────
    const { data: user, error: dbError } = await supabase
      .from('users')
      .select('id, full_name, goal, level, daily_time, is_active')
      .eq('telegram_id', telegramId)
      .single();

    // User not found or was deleted/banned by admin
    if (dbError || !user) {
      await ctx.reply(
        '⛔ Your account is not registered or has been suspended by the admin.\n' +
        'Please start from the website to create a new profile.'
      );
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    // Soft-ban: admin set is_active = false without deleting the row
    if (!user.is_active) {
      await ctx.reply('⛔ Your account has been deactivated. Contact support if this is a mistake.');
      isProcessing = false;
      setTimeout(processQueue, 1_000);
      return;
    }

    // ── Step B: Build full prompt with persona + JSON rules ──
    const fullPrompt = buildPrompt(user, userMessage);

    // ── Step C: Call Gemini, parse JSON response ─────────────
    const result  = await geminiModel.generateContent(fullPrompt);
    const rawText = result.response.text();

    let feedback, score;
    try {
      ({ feedback, score } = parseGeminiJSON(rawText));
    } catch (parseErr) {
      // ── Full debug dump so Render logs show exactly what Gemini returned ──
      console.error('━━━ [Queue] Gemini JSON parse failure ━━━');
      console.error('Error  :', parseErr.message);
      // JSON.stringify reveals invisible characters (newlines, BOM, etc.)
      console.error('Raw    :', JSON.stringify(rawText));
      console.error('Cleaned:', JSON.stringify(stripMarkdownFences(rawText)));
      console.error('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

      // Fallback: send the raw text if it has content, otherwise a safe message
      await ctx.reply(
        rawText && rawText.trim().length > 0
          ? rawText.trim()
          : '⚠️ I had trouble formatting my response. Please try again.'
      );
      isProcessing = false;
      setTimeout(processQueue, QUEUE_DELAY_MS);
      return;
    }

    // ── Step D: Reply with feedback only ────────────────────
    await ctx.reply(feedback);

    // ── Step E: Silently log score to progress_logs ──────────
    await logUserProgress(telegramId, score, extractTopic(userMessage));

    // ── Step F: Also update the existing progress table ──────
    await supabase.from('progress').insert({
      user_id:    user.id,
      topic:      extractTopic(userMessage),
      evaluation: `Score: ${score}/10`,
      mistakes:   [],
      timestamp:  new Date().toISOString(),
    });

  } catch (err) {
    console.error('[Queue] Processing error:', err?.message ?? err);

    // Distinguish Gemini rate-limit errors (429) from other failures
    const isRateLimit = err?.message?.includes('429') || err?.status === 429;
    if (isRateLimit) {
      await ctx.reply(
        '⏳ Fox is a little busy right now (too many lessons at once!).\n' +
        'Your request is queued — try again in 30 seconds.'
      );
      // Re-queue the item at the front so it retries after back-off
      messageQueue.unshift({ ctx, telegramId, userMessage });
      isProcessing = false;
      setTimeout(processQueue, 30_000);   // back-off 30 s on rate limit
      return;
    }

    await ctx.reply('⚠️ A technical error occurred. Please try again in a moment.');
  }

  isProcessing = false;
  setTimeout(processQueue, QUEUE_DELAY_MS);
}

// ─────────────────────────────────────────────────────────────
// 4. PROMPT BUILDER — full persona + JSON rules injected inline
//    (gemini-pro has no systemInstruction or generationConfig)
// ─────────────────────────────────────────────────────────────
function buildPrompt(user, userMessage) {
  const dailyMin = user.daily_time ?? 10;
  const fullPrompt = `You are a strict, encouraging AI micro-learning mentor. Your student's name is ${user.full_name}, their learning goal is "${user.goal}", their level is ${user.level}, and they have ${dailyMin} minutes today.

You MUST evaluate the following student message and reply ONLY with a raw JSON object containing exactly two keys: "feedback" (string: your correction, explanation, and next question — plain text, no HTML, same language as the student) and "score" (integer 0 to 10: how correct their answer was; use 5 if they asked a question instead of answering one). Do not include markdown formatting, code blocks, backticks, or any text outside the JSON object. Your entire reply must start with { and end with }.

Student's message: "${userMessage}"

Reply now with only the JSON object:`;
  return fullPrompt;
}

// ─────────────────────────────────────────────────────────────
// 5. HELPERS
// ─────────────────────────────────────────────────────────────

/** Naively extract a short topic label from the user message for progress logs. */
function extractTopic(message) {
  return message.slice(0, 80).trim();
}

/**
 * Strip all markdown code-block formatting from a Gemini response string
 * and return clean text ready for JSON.parse().
 *
 * Handles every variant Gemini produces in practice:
 *   ```json\n{...}\n```
 *   ```\n{...}\n```
 *   `{...}`          (single-backtick inline)
 *   {... }           (already clean — returned as-is)
 *
 * @param {string} raw
 * @returns {string}
 */
function stripMarkdownFences(raw) {
  let s = raw.trim();

  // Remove triple-backtick fences with optional language tag, e.g. ```json\n...\n```
  // The [\s\S]*? matches across newlines; the gi flags handle any capitalisation.
  const tripleMatch = s.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/gi);
  if (tripleMatch) {
    s = s.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
  }

  // Remove single-backtick wrapping, e.g. `{...}`
  if (s.startsWith('`') && s.endsWith('`')) {
    s = s.slice(1, -1);
  }

  return s.trim();
}

/**
 * Parse the raw Gemini text into { feedback, score }.
 *
 * @param {string} raw
 * @returns {{ feedback: string, score: number }}
 */
function parseGeminiJSON(raw) {
  const cleaned = stripMarkdownFences(raw);
  const parsed  = JSON.parse(cleaned);   // throws SyntaxError if still not valid JSON

  if (typeof parsed.feedback !== 'string' || parsed.feedback.trim() === '') {
    throw new Error('Gemini JSON missing "feedback" string.');
  }
  const score = parseInt(parsed.score, 10);
  if (isNaN(score) || score < 0 || score > 10) {
    throw new Error(`Gemini JSON "score" out of range: ${parsed.score}`);
  }
  return { feedback: parsed.feedback.trim(), score };
}

/**
 * Silently insert a scored session into progress_logs.
 * Failures are logged but never surface to the Telegram user.
 *
 * @param {number} telegramId
 * @param {number} score  0–10
 * @param {string} topic  short label extracted from the user message
 */
async function logUserProgress(telegramId, score, topic) {
  try {
    const { error } = await supabase
      .from('progress_logs')
      .insert({
        telegram_id: telegramId,
        score,
        topic:       topic.slice(0, 120),
        created_at:  new Date().toISOString(),
      });
    if (error) throw error;
  } catch (err) {
    // Silent — a logging failure must never interrupt the student's session
    console.error('[logUserProgress] Failed to log score:', err?.message ?? err);
  }
}

/** Fetch user by telegram_id — returns null if not found or banned. */
async function getActiveUser(telegramId) {
  const { data, error } = await supabase
    .from('users')
    .select('id, full_name, goal, level, is_active')
    .eq('telegram_id', telegramId)
    .single();
  if (error || !data || !data.is_active) return null;
  return data;
}

// ─────────────────────────────────────────────────────────────
// 6. /start — DEEP LINKING (website → Telegram account claim)
//
//    Flow: user completes onboarding on site → site creates a
//    users row with a temp telegram_id and returns the UUID →
//    site redirects to t.me/BOT?start=UUID →
//    this handler updates the row with the real telegram_id
//    and activates the account.
// ─────────────────────────────────────────────────────────────
bot.start(async (ctx) => {
  const supabaseUserId = ctx.startPayload;   // UUID passed via deep link
  const telegramId     = ctx.from.id;        // real Telegram numeric ID
  const firstName      = ctx.from.first_name ?? 'learner';

  // Plain /start — no deep-link payload
  if (!supabaseUserId) {
    return ctx.reply(
      `👋 Welcome to *Fox AI*!\n\n` +
      `To start learning, visit our website and complete the 60-second onboarding.\n` +
      `You'll be sent directly back here with your personalised plan. 🚀`,
      { parse_mode: 'Markdown' }
    );
  }

  try {
    // Check if this Telegram ID is already linked to ANY account
    const existingUser = await getActiveUser(telegramId);
    if (existingUser) {
      return ctx.reply(
        `✅ Your account is already linked, *${existingUser.full_name}*!\n\n` +
        `Just send me a message to continue learning about *${existingUser.goal}*. 🎯`,
        { parse_mode: 'Markdown' }
      );
    }

    // Claim the pre-created onboarding row
    const { data, error } = await supabase
      .from('users')
      .update({
        telegram_id: telegramId,
        is_active:   true,
      })
      .eq('id', supabaseUserId)
      .select('full_name, goal, level, daily_time')
      .single();

    if (error || !data) {
      console.error('[/start] Link error:', error?.message);
      return ctx.reply(
        "❌ I couldn't find your onboarding profile. Please go back to the website and try again.\n" +
        'The link may have expired or already been used.'
      );
    }

    const levelEmoji = { beginner: '🌱', intermediate: '📚', advanced: '🚀' }[data.level] ?? '📖';

    await ctx.reply(
      `🎉 *Account linked successfully, ${data.full_name}!*\n\n` +
      `Here's your study plan:\n` +
      `• 🎯 Goal: *${data.goal}*\n` +
      `• ${levelEmoji} Level: *${data.level}*\n` +
      `• ⏱ Daily time: *${data.daily_time} minutes*\n\n` +
      `Send me any question or topic related to your goal and I'll start coaching you.\n` +
      `Type *"Start"* to get your very first lesson! 🦊`,
      { parse_mode: 'Markdown' }
    );

  } catch (err) {
    console.error('[/start] Unexpected error:', err?.message ?? err);
    ctx.reply('⚠️ An internal error occurred during account linking. Please try again.');
  }
});

// ─────────────────────────────────────────────────────────────
// 7. /progress — show the user their session history
// ─────────────────────────────────────────────────────────────
bot.command('progress', async (ctx) => {
  const telegramId = ctx.from.id;

  const user = await getActiveUser(telegramId);
  if (!user) {
    return ctx.reply('⛔ Account not found. Visit the website to register.');
  }

  const { data: sessions, error } = await supabase
    .from('progress')
    .select('topic, evaluation, timestamp')
    .eq('user_id', user.id)
    .order('timestamp', { ascending: false })
    .limit(10);

  if (error || !sessions?.length) {
    return ctx.reply("You haven't completed any sessions yet. Send me a message to start! 🚀");
  }

  const lines = sessions.map((s, i) => {
    const date = new Date(s.timestamp).toLocaleDateString('en-GB', {
      day: '2-digit', month: 'short',
    });
    return `${i + 1}. *${s.topic.slice(0, 40)}* — ${date}`;
  });

  await ctx.reply(
    `📊 *Your last ${sessions.length} sessions, ${user.full_name}:*\n\n` +
    lines.join('\n') +
    '\n\nKeep it up! Consistency is everything. 💪',
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 8. /help — command reference
// ─────────────────────────────────────────────────────────────
bot.command('help', (ctx) => {
  ctx.reply(
    `🦊 *Fox AI — Commands*\n\n` +
    `/start — Link your website account\n` +
    `/progress — View your last 10 sessions\n` +
    `/help — Show this message\n\n` +
    `Or just *send any message* and Fox will coach you instantly.`,
    { parse_mode: 'Markdown' }
  );
});

// ─────────────────────────────────────────────────────────────
// 9. HANDLE INCOMING TEXT MESSAGES
// ─────────────────────────────────────────────────────────────
bot.on('text', async (ctx) => {
  const telegramId  = ctx.from.id;
  const userMessage = ctx.message.text.trim();

  // Guard: ignore empty messages
  if (!userMessage) return;

  // Guard: queue overflow protection
  if (messageQueue.length >= MAX_QUEUE_SIZE) {
    return ctx.reply(
      '⚠️ Fox is under heavy load right now. Please wait a moment and try again.'
    );
  }

  // Acknowledge receipt immediately (cheap — no Gemini call)
  const position = messageQueue.length + (isProcessing ? 1 : 0);
  const waitMsg  = position > 0
    ? `⏳ Analysing... (you are #${position + 1} in the queue)`
    : '⏳ Analysing your message...';

  await ctx.reply(waitMsg);

  // Push to queue
  messageQueue.push({ ctx, telegramId, userMessage });
  processQueue();
});

// ─────────────────────────────────────────────────────────────
// 10. GLOBAL ERROR HANDLERS
// ─────────────────────────────────────────────────────────────
bot.catch((err, ctx) => {
  console.error(`[Telegraf] Unhandled error for update ${ctx.updateType}:`, err?.message ?? err);
});

process.on('unhandledRejection', (reason) => {
  console.error('[Process] Unhandled rejection:', reason);
});

// ─────────────────────────────────────────────────────────────
// 11. HEALTH SERVER — required by Render Free Web Service
//
//    Render's free tier expects the process to bind a port and
//    respond to HTTP. This tiny Express server satisfies that
//    requirement without interfering with Telegraf polling.
// ─────────────────────────────────────────────────────────────
const app  = express();
const PORT = process.env.PORT || 3000;

app.get('/', (_req, res) => {
  res.send('🤖 Fox Bot is running — Telegraf polling is active.');
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🌐 Health server listening on 0.0.0.0:${PORT}`);
});

// ─────────────────────────────────────────────────────────────
// LAUNCH — POLLING MODE (for Render / Railway)
//    Polling is reliable on free-tier hosts where webhook
//    HTTPS certificates and static IPs are unavailable.
// ─────────────────────────────────────────────────────────────
// Delete any existing webhook and drop ghost sessions before polling starts.
// This is the definitive fix for 409 Conflict on Render redeploys.
bot.telegram.deleteWebhook({ drop_pending_updates: true })
  .then(() => console.log('✅ Webhook deleted — ghost sessions cleared.'))
  .catch((e) => console.warn('⚠️  deleteWebhook failed (non-fatal):', e.message));

bot.launch({
  allowedUpdates: ['message', 'callback_query'],
  dropPendingUpdates: true,
})
  .then(() => {
    console.log('🤖 Fox Telegram Bot Worker running in Polling mode...');
    console.log(`📋 Queue capacity: ${MAX_QUEUE_SIZE} messages`);
    console.log(`⏱  Gemini rate-limit delay: ${QUEUE_DELAY_MS / 1000}s between calls`);
  })
  .catch((err) => {
    console.error('❌ Failed to launch bot:', err?.message ?? err);
    process.exit(1);
  });

// Graceful shutdown — stops Telegraf polling before the process exits,
// preventing a 409 Conflict on the next deploy when two instances overlap.
process.once('SIGINT',  () => {
  console.log('Shutting down (SIGINT)…');
  bot.stop('SIGINT');
});
process.once('SIGTERM', () => {
  console.log('Shutting down (SIGTERM)…');
  bot.stop('SIGTERM');
});
