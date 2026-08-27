import asyncio
import typing
from typing import List, Dict

import discord
from discord import Color, ui

import config
from role_system.fashion.fashion_config_manager import FashionConfigManager
from role_system.fashion.fashion_config_models import FashionMapEntry
from utility.auth import is_role_dangerous

if typing.TYPE_CHECKING:
    from role_system.fashion.FashionCog import FashionCog
from utility.helpers import safe_defer
from utility.role_service import update_member_roles
from shared.ui.paginated_view import PaginatedView

FASHION_ROLES_PER_PAGE = 25


class FashionManageView(PaginatedView):
    """用户私有的幻化身份组管理视图，继承自新版 PaginatedView。"""

    def __init__(self, cog: 'FashionCog', user: discord.Member):
        self.cog = cog
        self.user = user
        self.guild = user.guild

        # 1. 准备数据
        safe_fashion_map = self.cog.safe_fashion_map_cache.get(self.guild.id, {})
        self.fashion_to_base_map: Dict[int, List[int]] = {}
        # fashion_to_entries: fashion_id → 包含该幻化组的所有 entry（同一幻化组可能被
        # 多个 entry 引用）。UI 过滤「锁定后是否隐藏」时取所有 entry 的 AND：
        # 任意 entry 是 hidden_when_locked = false → 不隐藏。
        self.fashion_to_entries: Dict[int, List[FashionMapEntry]] = {}
        all_fashion_options = []

        # 从 toml 读完整 entries（safe_fashion_map_cache 已过滤危险角色，但丢了
        # entry 自己的 hidden_when_locked 字段；UI 行为要从原 entries 拿）。
        fashion_cfg = FashionConfigManager.get_instance().get(self.guild.id)
        safe_entries: list[FashionMapEntry] = []
        if fashion_cfg is not None:
            safe_base_ids = set(safe_fashion_map.keys())
            # 仅保留「至少有一个 safe fashion」的 entry——cog 的 cache 已经做了安全过滤。
            safe_entries = [
                e for e in fashion_cfg.fashion_map
                if e.base_role_id in safe_base_ids
            ]

        temp_fashion_to_bases: Dict[int, set[int]] = {}
        for entry in safe_entries:
            for fashion_id in entry.fashion_role_ids:
                if fashion_id not in temp_fashion_to_bases:
                    temp_fashion_to_bases[fashion_id] = set()
                temp_fashion_to_bases[fashion_id].add(entry.base_role_id)
                self.fashion_to_entries.setdefault(fashion_id, []).append(entry)

        for fashion_id, base_ids_set in temp_fashion_to_bases.items():
            self.fashion_to_base_map[fashion_id] = list(base_ids_set)
            all_fashion_options.append((fashion_id, list(base_ids_set)[0]))

        all_fashion_options.sort(key=lambda x: self.cog.role_name_cache.get(x[0], ''))

        if not all_fashion_options:
            self.cog.logger.info(f"服务器 {self.guild.id} 未配置幻化系统或无安全幻化组。")

        # 2. 调用父类构造函数，只传递数据，不传递 interaction/cog/user
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        get_all_fashion_options = lambda: all_fashion_options
        super().__init__(
            all_items_provider=get_all_fashion_options,
            items_per_page=FASHION_ROLES_PER_PAGE,
            timeout=timeout_minutes * 60
        )

    # 实现新的抽象方法 _rebuild_view
    async def _rebuild_view(self):
        self.clear_items()

        # 尝试安全地获取最新的成员对象
        member = self.guild.get_member(self.user.id)
        if member is None:
            self.embed = discord.Embed(title="错误", description="无法加载您的信息，您可能已离开服务器。", color=Color.red())
            self.add_item(ui.Button(label="错误", style=discord.ButtonStyle.danger, disabled=True))
            self.stop()
            return

        # --- 以下是原来 _rebuild_view 的逻辑 ---
        member_role_ids = {role.id for role in member.roles}

        self.embed = self.cog.get_guide_embed(self.guild.id)

        if not self.all_items:
            self.embed.description = "此服务器未配置幻化系统，或所有幻化身份组均不安全。"

        self.embed.set_footer(text=f"面板将在 {config.ROLE_MANAGER_CONFIG.get('private_panel_timeout_minutes', 3)} 分钟后失效。")

        page_fashion_options = self.get_page_items()

        self.add_item(FashionRoleSelect(
            self.cog, self.guild.id,
            fashion_to_base_map=self.fashion_to_base_map,
            fashion_to_entries=self.fashion_to_entries,
            page_options_data=page_fashion_options,
            member_role_ids=member_role_ids,
            page_num=self.page, total_pages=self.total_pages,
        ))

        # 从基类添加分页按钮
        self._add_pagination_buttons(row=1)

        # 跳转按钮已删除：embed_guides.toml 是 source of truth，不再有 Discord 跳转 URL。


class FashionRoleSelect(ui.Select):
    """幻化身份组的选择菜单，会根据用户是否拥有基础组来显示锁定/解锁状态。

    「锁定后是否隐藏」现在从 entry.hidden_when_locked 字段读取——每条 entry 自带
    UI 行为属性，不再依赖外部全局 not_normal_role_ids 列表。同一 fashion_id 可能
    被多个 entry 引用（不同 base_role 都能解锁同一个幻化组）；采用 AND 语义：
    所有 entry 都是 hidden_when_locked = true 才隐藏，任意 entry 是 false 就不藏。
    """

    def __init__(self, cog: 'FashionCog', guild_id: int,
                 fashion_to_base_map: Dict[int, List[int]],
                 fashion_to_entries: Dict[int, List[FashionMapEntry]],
                 page_options_data: List[tuple[int, int]],
                 member_role_ids: set[int], page_num: int, total_pages: int):
        self.cog = cog
        self.guild_id = guild_id
        self.fashion_to_base_map = fashion_to_base_map
        self.fashion_to_entries = fashion_to_entries

        sorted_page_options_data = sorted(page_options_data,
                                          key=lambda x: any(base_id in member_role_ids for base_id in self.fashion_to_base_map.get(x[0], [])),
                                          reverse=True)

        options = []
        for fashion_id, _ in sorted_page_options_data:
            fashion_name = cog.role_name_cache.get(fashion_id, f"未知(ID:{fashion_id})")
            required_base_ids = self.fashion_to_base_map.get(fashion_id, [])

            is_unlocked = any(base_id in member_role_ids for base_id in required_base_ids)

            # --- UI 过滤逻辑（OO 化后）---
            # 锁定 + 用户也没持有该幻化 + 所有引用它的 entry 都是 hidden_when_locked = true
            # → 隐藏（典型：Server Boosted → 幻化-Server Booster，未持有 boost 的普通用户看不到）
            if not is_unlocked and fashion_id not in member_role_ids:
                entries = self.fashion_to_entries.get(fashion_id, [])
                if entries and all(e.hidden_when_locked for e in entries):
                    continue  # 跳过，不渲染此选项
            # --- 过滤逻辑结束 ---

            label_prefix = "✅ " if is_unlocked else "🔒 "
            description_text = ""
            if is_unlocked:
                owned_base_ids = [bid for bid in required_base_ids if bid in member_role_ids]
                if owned_base_ids:
                    base_names = [cog.role_name_cache.get(bid, f"ID:{bid}") for bid in owned_base_ids]
                    description_text = f"由 {' 和 '.join(f'「{name}」' for name in base_names if name)}解锁"
            else:
                display_base_ids = [bid for bid in required_base_ids]
                if display_base_ids:
                    base_names = [cog.role_name_cache.get(bid, f"ID:{bid}") for bid in display_base_ids]
                    if len(base_names) == 1:
                        description_text = f"需要 {' 或 '.join(f'「{name}」' for name in base_names if name)}"
                    else:
                        description_text = f"需要 {' 或 '.join(f'「{name}」' for name in base_names if name)}中任意一个"

            options.append(
                discord.SelectOption(
                    label=f"{label_prefix}{fashion_name}",
                    value=str(fashion_id),
                    description=description_text,
                    default=(fashion_id in member_role_ids)
                )
            )

        # 优化后的占位符逻辑
        placeholder = f"幻化 (第 {page_num + 1}/{total_pages} 页)" if total_pages > 1 else "选择你的幻化"
        safe_fashion_map = self.cog.safe_fashion_map_cache.get(guild_id, {})

        if not safe_fashion_map:
            placeholder = "本服未配置幻化系统"
        elif not options and page_options_data:
            placeholder = "幻化名称加载中..."
        elif not options:
            has_any_base_role = any(base_id in member_role_ids for base_id in safe_fashion_map.keys())
            if not has_any_base_role:
                placeholder = "你没有可幻化的基础身份组"
            else:
                placeholder = "本页无你的可用幻化"
        else:
            placeholder += " (✅=可佩戴, 🔒=未解锁)"

        super().__init__(
            placeholder=placeholder, min_values=0, max_values=len(options) if options else 1,
            options=options if options else [discord.SelectOption(label="无可用选项", value="_placeholder", default=False)],
            custom_id="private_fashion_role_select", disabled=not options, row=0
        )

    async def callback(self, interaction: discord.Interaction):
        """处理幻化身份组选择后的回调逻辑，包括权限检查、身份组增删及用户反馈。"""
        await safe_defer(interaction)
        member, guild = interaction.user, interaction.guild

        fashion_to_base_map = self.view.fashion_to_base_map
        all_fashion_role_ids = set(fashion_to_base_map.keys())

        member_role_ids = {r.id for r in member.roles}
        old_selection_set = member_role_ids.intersection(all_fashion_role_ids)

        new_selection_in_page = {int(v) for v in self.values if v != "_placeholder"}
        options_in_this_page_ids = {int(opt.value) for opt in self.options if opt.value != "_placeholder"}
        selections_not_in_this_page = old_selection_set - options_in_this_page_ids
        final_new_selection_set = selections_not_in_this_page.union(new_selection_in_page)

        roles_to_add_ids = final_new_selection_set - old_selection_set
        roles_to_remove_ids = old_selection_set - final_new_selection_set

        roles_to_actually_add, roles_to_actually_remove = [], []
        failed_attempts = []

        for role_id in roles_to_add_ids:
            required_base_ids = fashion_to_base_map.get(role_id, [])
            if required_base_ids and any(base_id in member_role_ids for base_id in required_base_ids):
                role_obj = guild.get_role(role_id)
                if role_obj and not is_role_dangerous(role_obj):
                    roles_to_actually_add.append(role_obj)
                else:
                    self.cog.logger.warning(f"用户 {member.id} 尝试获取危险/不存在的幻化 {role_id}，已阻止。")
            else:
                role_name = self.cog.role_name_cache.get(role_id, f"ID:{role_id}")
                if required_base_ids:
                    base_names = [self.cog.role_name_cache.get(bid, f"ID:{bid}") for bid in required_base_ids]
                    failed_attempts.append(f"**{role_name}** (需要 {' 或 '.join(f'**{name}**' for name in base_names if name)} 中任意一个)")
                else:
                    failed_attempts.append(f"**{role_name}** (不满足特殊解锁条件)")

        for role_id in roles_to_remove_ids:
            role_obj = guild.get_role(role_id)
            if role_obj: roles_to_actually_remove.append(role_obj)

        if roles_to_actually_add or roles_to_actually_remove:
            await update_member_roles(
                cog=self.cog,
                member=member,
                to_add_ids={r.id for r in roles_to_actually_add},
                to_remove_ids={r.id for r in roles_to_actually_remove},
                reason="自助幻化操作"
            )

        if failed_attempts:
            warning_message = await interaction.followup.send(
                f"❌ 操作部分成功。\n你无法佩戴以下幻化，因为你缺少必需的基础身份组：\n- " + "\n- ".join(failed_attempts),
                ephemeral=True
            )
            await asyncio.sleep(5)
            await warning_message.delete()

        if isinstance(self.view, PaginatedView):
            await self.view.update_view(interaction)
