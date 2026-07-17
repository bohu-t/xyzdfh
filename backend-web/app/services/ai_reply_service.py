"""
AI回复设置服务

功能：
1. 管理账号的AI回复配置
2. 支持全局默认AI配置，账号未单独配置时自动继承
3. 支持模型名称、API密钥、折扣设置等
"""
from __future__ import annotations

import json

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from common.services.ai_provider_service import (
    DEFAULT_AI_BASE_URL,
    DEFAULT_AI_PROVIDER_TYPE,
    clean_ai_text,
    get_ai_settings_missing_fields,
    normalize_ai_provider_type,
    read_ai_enabled,
)
from common.models.xy_account import XYAccount
from common.models.system_setting import SystemSetting

GLOBAL_AI_SETTINGS_KEY = "ai.reply.default_settings"

DEFAULT_AI_SETTINGS = {
    "ai_enabled": False,
    "provider_type": DEFAULT_AI_PROVIDER_TYPE,
    "model_name": "qwen-plus",
    "api_key": "",
    "base_url": DEFAULT_AI_BASE_URL,
    "max_discount_percent": 10,
    "max_discount_amount": 100,
    "max_bargain_rounds": 3,
    "custom_prompts": "",
    "ai_time_range_start": "",
    "ai_time_range_end": "",
}

ACCOUNT_ONLY_KEYS = {"ai_enabled", "enabled"}


class AIReplySettingsService:
    """Stores account AI switch in account metadata and shared defaults globally."""

    def __init__(self, session: AsyncSession):
        self.session = session

    def _normalize_settings(self, settings: dict | None, *, enabled_source: dict | None = None) -> dict:
        source = dict(settings or {})
        payload = DEFAULT_AI_SETTINGS.copy()
        payload.update({k: v for k, v in source.items() if v is not None})
        enabled_payload = enabled_source if enabled_source is not None else source
        payload["ai_enabled"] = read_ai_enabled(enabled_payload)
        payload["max_discount_percent"] = int(payload.get("max_discount_percent", 10) or 0)
        payload["max_discount_amount"] = int(payload.get("max_discount_amount", 100) or 0)
        payload["max_bargain_rounds"] = int(payload.get("max_bargain_rounds", 3) or 0)
        payload["provider_type"] = normalize_ai_provider_type(
            payload.get("provider_type"),
            payload.get("base_url"),
            payload.get("model_name"),
        )
        payload["model_name"] = clean_ai_text(payload.get("model_name"))
        payload["api_key"] = clean_ai_text(payload.get("api_key"))
        payload["base_url"] = clean_ai_text(payload.get("base_url"))
        payload["custom_prompts"] = payload.get("custom_prompts") or ""
        payload["ai_time_range_start"] = payload.get("ai_time_range_start") or ""
        payload["ai_time_range_end"] = payload.get("ai_time_range_end") or ""
        payload["enabled"] = payload["ai_enabled"]
        return payload

    async def _get_global_defaults(self) -> dict:
        stmt = select(SystemSetting.value).where(SystemSetting.key == GLOBAL_AI_SETTINGS_KEY)
        result = await self.session.execute(stmt)
        raw = result.scalar_one_or_none()
        if not raw:
            return DEFAULT_AI_SETTINGS.copy()
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return DEFAULT_AI_SETTINGS.copy()
            defaults = {k: v for k, v in parsed.items() if k not in ACCOUNT_ONLY_KEYS}
            return self._normalize_settings(defaults, enabled_source={"ai_enabled": False})
        except Exception:
            return DEFAULT_AI_SETTINGS.copy()

    async def _save_global_defaults(self, settings: dict) -> dict:
        defaults = {k: v for k, v in settings.items() if k not in ACCOUNT_ONLY_KEYS}
        defaults = self._normalize_settings(defaults, enabled_source={"ai_enabled": False})
        defaults.pop("ai_enabled", None)
        defaults.pop("enabled", None)
        raw = json.dumps(defaults, ensure_ascii=False)
        stmt = select(SystemSetting).where(SystemSetting.key == GLOBAL_AI_SETTINGS_KEY)
        result = await self.session.execute(stmt)
        record = result.scalar_one_or_none()
        if record:
            record.value = raw
            record.description = "全局默认AI回复配置；账号未单独配置时继承"
        else:
            self.session.add(SystemSetting(
                key=GLOBAL_AI_SETTINGS_KEY,
                value=raw,
                description="全局默认AI回复配置；账号未单独配置时继承",
            ))
        return defaults

    def _merge_account_switch_with_defaults(self, account: XYAccount, defaults: dict) -> dict:
        metadata = dict(account.metadata_json or {})
        stored = dict(metadata.get("ai_reply_settings") or {})
        enabled = read_ai_enabled(stored)
        merged = DEFAULT_AI_SETTINGS.copy()
        merged.update({k: v for k, v in defaults.items() if k not in ACCOUNT_ONLY_KEYS and v is not None})
        # 保留账号自己的开启/关闭状态；服务商/API Key/模型等统一来自全局默认。
        merged["ai_enabled"] = enabled
        merged["enabled"] = enabled
        metadata["ai_reply_settings"] = merged
        return metadata

    async def _apply_global_defaults_to_owner_accounts(self, owner_id: int, defaults: dict) -> None:
        stmt = select(XYAccount).where(XYAccount.owner_id == owner_id)
        result = await self.session.execute(stmt)
        accounts = result.scalars().all()
        for account in accounts:
            metadata = self._merge_account_switch_with_defaults(account, defaults)
            await self.session.execute(
                update(XYAccount)
                .where(XYAccount.id == account.id)
                .values(metadata_json=metadata)
            )
            account.metadata_json = metadata

    async def _extract_settings(self, account: XYAccount) -> dict:
        stored = (account.metadata_json or {}).get("ai_reply_settings") or {}
        defaults = await self._get_global_defaults()
        inherited = DEFAULT_AI_SETTINGS.copy()
        inherited.update({k: v for k, v in defaults.items() if k not in ACCOUNT_ONLY_KEYS and v is not None})
        inherited.update({k: v for k, v in stored.items() if v is not None})
        return self._normalize_settings(inherited, enabled_source=stored)

    async def get_settings(self, account: XYAccount) -> dict:
        return await self._extract_settings(account)

    async def update_settings(self, account: XYAccount, payload: dict) -> dict:
        existing = await self._extract_settings(account)

        merged = existing.copy()
        if "ai_enabled" in payload:
            merged["ai_enabled"] = bool(payload.get("ai_enabled"))
        elif "enabled" in payload:
            merged["ai_enabled"] = bool(payload.get("enabled"))
        if "provider_type" in payload:
            merged["provider_type"] = normalize_ai_provider_type(
                payload.get("provider_type"),
                payload.get("base_url") or merged.get("base_url"),
                payload.get("model_name") or merged.get("model_name"),
            )
        if "model_name" in payload:
            merged["model_name"] = clean_ai_text(payload.get("model_name"))
        if "api_key" in payload:
            merged["api_key"] = clean_ai_text(payload.get("api_key"))
        if "base_url" in payload:
            merged["base_url"] = clean_ai_text(payload.get("base_url"))
        if "max_discount_percent" in payload:
            merged["max_discount_percent"] = int(payload.get("max_discount_percent", 10) or 0)
        if "max_discount_amount" in payload:
            merged["max_discount_amount"] = int(payload.get("max_discount_amount", 100) or 0)
        if "max_bargain_rounds" in payload:
            merged["max_bargain_rounds"] = int(payload.get("max_bargain_rounds", 3) or 0)
        if "custom_prompts" in payload:
            merged["custom_prompts"] = payload.get("custom_prompts") or ""
        if "ai_time_range_start" in payload:
            merged["ai_time_range_start"] = payload.get("ai_time_range_start") or ""
        if "ai_time_range_end" in payload:
            merged["ai_time_range_end"] = payload.get("ai_time_range_end") or ""
        merged = self._normalize_settings(merged, enabled_source=merged)
        if merged.get("ai_enabled"):
            missing_fields = get_ai_settings_missing_fields(merged)
            if missing_fields:
                raise ValueError(f"AI配置未填写完整，请先补全：{'、'.join(missing_fields)}")

        config_changed = any(k not in ACCOUNT_ONLY_KEYS for k in payload.keys())
        defaults = None
        if config_changed:
            # 任意账号里更新服务商/API Key/模型/提示词等配置时，同时保存为全局默认，
            # 并同步到同一用户的所有账号；每个账号只保留自己的开启/关闭状态。
            defaults = await self._save_global_defaults(merged)
            await self._apply_global_defaults_to_owner_accounts(account.owner_id, defaults)

        metadata = dict(account.metadata_json or {})
        account_settings = dict(metadata.get("ai_reply_settings") or {})
        if config_changed and defaults is not None:
            account_settings = DEFAULT_AI_SETTINGS.copy()
            account_settings.update(defaults)
        # 账号级开关仍独立保存。
        account_settings["ai_enabled"] = merged["ai_enabled"]
        account_settings["enabled"] = merged["ai_enabled"]
        if not config_changed:
            # 只切换开关时，保留已有/继承后的完整快照，确保运行端立刻可用。
            for key, value in merged.items():
                account_settings[key] = value
            account_settings["ai_enabled"] = merged["ai_enabled"]
            account_settings["enabled"] = merged["ai_enabled"]
        metadata["ai_reply_settings"] = account_settings
        stmt = (
            update(XYAccount)
            .where(XYAccount.id == account.id)
            .values(metadata_json=metadata)
        )
        await self.session.execute(stmt)
        await self.session.commit()
        account.metadata_json = metadata
        return merged

    async def list_settings(self, owner_id: int) -> dict[str, dict]:
        stmt = select(XYAccount).where(XYAccount.owner_id == owner_id)
        result = await self.session.execute(stmt)
        accounts = result.scalars().all()
        return {account.account_id: await self._extract_settings(account) for account in accounts}
