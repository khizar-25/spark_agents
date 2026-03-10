# agent/llm_reasoner.py
import json, logging, os, time, requests
from typing import Any, Dict, List
from config.settings import OPENROUTER_API_KEY, LLM_MAX_TOKENS, LLM_TEMPERATURE, MAX_ITERATIONS
from config.prompts import SYSTEM_PROMPT, GOAL_INJECTION_PROMPT, DIAGNOSE_PROMPT, GOAL_REACHED_PROMPT, OPTIMIZE_PROMPT

logger = logging.getLogger(__name__)
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

FALLBACK_MODELS = [
    "openrouter/auto",
    "meta-llama/llama-3.3-70b-instruct:free",
    "deepseek/deepseek-r1:free",
    "google/gemini-2.0-flash-exp:free",
    "deepseek/deepseek-r1-distill-llama-70b:free",
    "meta-llama/llama-3.1-8b-instruct:free",
]

class LLMReasoner:
    def __init__(self, tool_registry: Dict[str, Any]):
        self.api_key = os.environ.get("OPENROUTER_API_KEY", OPENROUTER_API_KEY)
        self.tool_registry = tool_registry
        self._conversation_history: List[Dict] = []

    def decide_next_action(self, current_state, goal_description, action_history, iteration):
        tool_list = "\n".join(f"  - {n}: {m['description']}" for n, m in self.tool_registry.items())
        tool_list += "\n  - goal_complete: Call this when the pipeline goal has been fully achieved"
        system = SYSTEM_PROMPT.format(tool_list=tool_list)
        user_msg = GOAL_INJECTION_PROMPT.format(
            goal_description=goal_description,
            current_state=json.dumps(current_state, indent=2),
            iteration=iteration, max_iterations=MAX_ITERATIONS,
            action_history=json.dumps(action_history[-5:], indent=2),
        )
        self._conversation_history.append({"role": "user", "content": user_msg})
        response = self._call_with_fallback(system, self._conversation_history)
        parsed = self._parse_json_response(response)
        self._conversation_history.append({"role": "assistant", "content": response})
        logger.info(f"[LLMReasoner] tool={parsed.get('tool_name')} confidence={parsed.get('confidence',0):.2f}")
        return parsed

    def diagnose(self, current_state, error_history, goal_description):
        prompt = DIAGNOSE_PROMPT.format(
            current_state=json.dumps(current_state, indent=2),
            error_history=json.dumps(error_history, indent=2),
            goal_description=goal_description,
        )
        r = self._call_with_fallback("You are a Spark diagnostics expert.", [{"role": "user", "content": prompt}])
        return self._parse_json_response(r)

    def evaluate_goal_reached(self, goal_description, final_state, metrics):
        prompt = GOAL_REACHED_PROMPT.format(
            goal_description=goal_description,
            final_state=json.dumps(final_state, indent=2),
            metrics=json.dumps(metrics, indent=2),
        )
        r = self._call_with_fallback("You are a pipeline auditor.", [{"role": "user", "content": prompt}])
        return self._parse_json_response(r)

    def suggest_optimizations(self, execution_plan, metrics, data_size_gb, cluster_config):
        prompt = OPTIMIZE_PROMPT.format(
            execution_plan=execution_plan,
            metrics=json.dumps(metrics, indent=2),
            data_size_gb=data_size_gb,
            cluster_config=json.dumps(cluster_config, indent=2),
        )
        r = self._call_with_fallback("You are a Spark performance expert.", [{"role": "user", "content": prompt}])
        return self._parse_json_response(r)

    def _call_with_fallback(self, system, messages):
        for i, model in enumerate(FALLBACK_MODELS):
            try:
                logger.info(f"[LLMReasoner] Trying model: {model}")
                result = self._call_api(system, messages, model)
                logger.info(f"[LLMReasoner] SUCCESS with: {model}")
                return result
            except requests.exceptions.HTTPError as e:
                code = e.response.status_code
                logger.warning(f"[LLMReasoner] {model} HTTP {code}. Trying next...")
                time.sleep(2 + i)
                continue
            except Exception as e:
                logger.warning(f"[LLMReasoner] {model} failed: {e}. Trying next...")
                time.sleep(2)
                continue
        raise Exception("All LLM models failed. Check OPENROUTER_API_KEY in .env file.")

    def _call_api(self, system, messages, model):
        if not self.api_key or self.api_key.startswith("sk-or-v1-your"):
            raise ValueError("OPENROUTER_API_KEY not set! Run: python3 setup.py")
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/khizar-25/spark-agent",
            "X-Title": "SparkAgent",
        }
        payload = {
            "model": model,
            "max_tokens": LLM_MAX_TOKENS,
            "temperature": LLM_TEMPERATURE,
            "messages": [{"role": "system", "content": system}] + messages,
        }
        response = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]

    def _parse_json_response(self, text):
        try:
            clean = text.strip()
            if "```" in clean:
                parts = clean.split("```")
                for p in parts:
                    p = p.strip()
                    if p.startswith("json"):
                        p = p[4:]
                    try:
                        return json.loads(p.strip())
                    except:
                        continue
            return json.loads(clean)
        except:
            return {"tool_name": "diagnose_pipeline", "tool_args": {}, "reasoning": text[:200], "confidence": 0.1, "estimated_goal_progress": 0.0}

    def reset_history(self):
        self._conversation_history = []
