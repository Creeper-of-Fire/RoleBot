from typing import TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from .anniversary_module import HonorAnniversaryModuleCog
    from .role_sync_honor_module import RoleClaimHonorModuleCog
    from .cog import HonorCog
    from utility.feature_cog import FeatureCog

def getHonorAnniversaryModuleCog(cog: 'FeatureCog') -> 'HonorAnniversaryModuleCog | None':
    anniversary_cog: Optional['HonorAnniversaryModuleCog'] = cog.bot.get_cog("HonorAnniversaryModule")
    return anniversary_cog

def getRoleClaimHonorModuleCog(cog: 'FeatureCog') -> 'RoleClaimHonorModuleCog | None':
    role_claim_cog: Optional['RoleClaimHonorModuleCog'] = cog.bot.get_cog("RoleClaimHonorModule")
    return role_claim_cog

def getHonorCog(cog: 'FeatureCog') -> 'HonorCog | None':
    honor_cog: Optional['HonorCog'] = cog.bot.get_cog("Honor")
    return honor_cog