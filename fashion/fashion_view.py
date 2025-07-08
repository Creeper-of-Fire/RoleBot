import asyncio
import typing
from typing import List, Dict

import discord
from discord import Color, ui

import config
import config_data
from utility.auth import is_role_dangerous

if typing.TYPE_CHECKING:
    from fashion.cog import FashionCog
from utility.helpers import try_get_member, safe_defer
from utility.role_service import update_member_roles
from utility.paginated_view import PaginatedView

FASHION_ROLES_PER_PAGE = 25


class FashionManageView(PaginatedView):
    """用户私有的幻化身份组管理视图，继承自 PaginatedView。"""

    def __init__(self, cog: 'FashionCog', user: discord.Member):
        timeout_minutes = config.ROLE_MANAGER_CONFIG.get("private_panel_timeout_minutes", 3)
        super().__init__(cog, user, items_per_page=FASHION_ROLES_PER_PAGE, timeout=timeout_minutes * 60)
        self.cog = cog

        safe_fashion_map = self.cog.safe_fashion_map_cache.get(self.guild.id, {})
        self.fashion_to_base_map: Dict[int, List[int]] = {}
        all_fashion_options = []

        temp_fashion_to_bases: Dict[int, set[int]] = {}
        for base_id, fashion_ids_list in safe_fashion_map.items():
            for fashion_id in fashion_ids_list:
                if fashion_id not in temp_fashion_to_bases:
                    temp_fashion_to_bases[fashion_id] = set()
                temp_fashion_to_bases[fashion_id].add(base_id)

        for fashion_id, base_ids_set in temp_fashion_to_bases.items():
            self.fashion_to_base_map[fashion_id] = list(base_ids_set)
            representative_base_id = list(base_ids_set)[0]
            all_fashion_options.append((fashion_id, representative_base_id))

        all_fashion_options.sort(key=lambda x: self.cog.role_name_cache.get(x[0], ''))
        self._update_page_info(all_fashion_options)

        if not self.all_items:
            self.cog.logger.info(f"服务器 {self.guild.id} 未配置幻化系统或无安全幻化组。")

    async def _rebuild_view(self):
        self.clear_items()
        member = self._try_get_safe_member()
        if member is None:
            return

        member_role_ids = {role.id for role in member.roles}
        all_configured_base_ids = set(self.cog.safe_fashion_map_cache.get(self.guild.id, {}).keys())
        member_base_role_ids = member_role_ids.intersection(all_configured_base_ids)

        booster_role_ids = set(getattr(config_data, "FASHION_BOOSTER_ROLE_IDS", []))
        non_booster_base_role_ids = member_base_role_ids - booster_role_ids

        # 如果用户没有任何非赞助的基础身份组，则显示指引
        if not non_booster_base_role_ids:
            self.embed = self.cog.guide_embed
        else:
            self.embed = discord.Embed(title=f"👗 {self.user.display_name} 的幻化衣橱", color=Color.green())
            self.embed.description = "在这里管理你的幻化外观吧！"

        if not self.all_items:
            # 如果服务器没有任何幻化项，则覆盖Embed描述
            self.embed.description = "此服务器未配置幻化系统，或所有幻化身份组均不安全。"

        self.embed.set_footer(text=f"面板将在 {config.ROLE_MANAGER_CONFIG.get('private_panel_timeout_minutes', 3)} 分钟后失效。")

        start, end = self.get_page_range()
        page_fashion_options = self.all_items[start:end]

        self.add_item(FashionRoleSelect(
            self.cog, self.guild.id,
            fashion_to_base_map=self.fashion_to_base_map,
            page_options_data=page_fashion_options,
            all_role_ids=member_role_ids,
            page_num=self.page, total_pages=self.total_pages,
        ))

        self._add_pagination_buttons(row=1)

        # --- 为所有情况添加指引链接按钮 ---
        if self.cog.guide_url:  # 只有当 URL 成功缓存时才添加按钮
            self.add_item(ui.Button(
                label="跳转到 “" + self.cog.guide_embed.title + "”",
                style=discord.ButtonStyle.link,
                url=self.cog.guide_url,
                row=2  # 放在新的一行，避免与分页按钮挤占
            ))


class FashionRoleSelect(ui.Select):
    """幻化身份组的选择菜单，会根据用户是否拥有基础组来显示锁定/解锁状态。"""

    def __init__(self, cog: 'FashionCog', guild_id: int, fashion_to_base_map: Dict[int, List[int]], page_options_data: List[tuple[int, int]],
                 all_role_ids: set[int], page_num: int, total_pages: int):
        self.cog = cog
        self.guild_id = guild_id
        self.fashion_to_base_map = fashion_to_base_map
        self.booster_role_ids = set(getattr(config_data, "FASHION_BOOSTER_ROLE_IDS", []))

        sorted_page_options_data = sorted(page_options_data, key=lambda x: any(base_id in all_role_ids for base_id in self.fashion_to_base_map.get(x[0], [])),
                                          reverse=True)

        options = []
        for fashion_id, _ in sorted_page_options_data:
            fashion_name = cog.role_name_cache.get(fashion_id, f"未知(ID:{fashion_id})")
            required_base_ids = self.fashion_to_base_map.get(fashion_id, [])

            is_unlocked = any(base_id in all_role_ids for base_id in required_base_ids)

            # --- 新增的过滤逻辑 ---
            # 如果幻化是锁定的，并且其所有解锁条件都是赞助身份组，则不向该用户显示此选项
            if not is_unlocked:
                is_booster_only_unlock = required_base_ids and all(bid in self.booster_role_ids for bid in required_base_ids)
                if is_booster_only_unlock:
                    continue  # 跳过，不渲染此选项
            # --- 过滤逻辑结束 ---

            label_prefix = "✅ " if is_unlocked else "🔒 "
            description_text = ""
            if is_unlocked:
                owned_base_id = next((bid for bid in required_base_ids if bid in all_role_ids), None)
                base_name = cog.role_name_cache.get(owned_base_id, "未知基础组")
                description_text = f"由「{base_name}」解锁"
            else:
                display_base_ids = [bid for bid in required_base_ids if bid not in self.booster_role_ids]
                if display_base_ids:
                    base_names = [cog.role_name_cache.get(bid, f"ID:{bid}") for bid in display_base_ids]
                    description_text = f"需要拥有 {' 或 '.join(f'「{name}」' for name in base_names if name)}中任意一个"

            options.append(
                discord.SelectOption(
                    label=f"{label_prefix}{fashion_name}",
                    value=str(fashion_id),
                    description=description_text,
                    default=(fashion_id in all_role_ids)
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
            has_any_base_role = any(base_id in all_role_ids for base_id in safe_fashion_map.keys())
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
                display_base_ids = [bid for bid in required_base_ids if bid not in self.booster_role_ids]
                if display_base_ids:
                    base_names = [self.cog.role_name_cache.get(bid, f"ID:{bid}") for bid in display_base_ids]
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

        refreshed_member = await try_get_member(guild, member.id)
        if refreshed_member:
            new_view = FashionManageView(self.cog, refreshed_member)
            await new_view._rebuild_view()
            if interaction.response.is_done():
                await interaction.edit_original_response(content=None, embed=new_view.embed, view=new_view)
            else:
                # This case is unlikely but safe to handle
                await interaction.followup.send(content=None, embed=new_view.embed, view=new_view, ephemeral=True)
        else:
            await interaction.edit_original_response(content="无法刷新你的信息，请重试。", view=None, embed=None)
