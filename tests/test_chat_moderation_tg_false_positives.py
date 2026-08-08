import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from module_chat_moderator import router as club_module
from module_chat_moderators import router as training_module


FALSE_POSITIVES = (
    "прошлый пёс готов был часами просить выскуливать и манипулировать жалобным свистом))",
    "Помню что запретов много было на информацию о чем-то и на ругань 🤬",
    "Да жидковат",
)


class TelegramNegativeGuardTests(unittest.TestCase):
    def test_ai_negative_requires_real_negative_evidence(self) -> None:
        for module in (club_module, training_module):
            with self.subTest(module=module.MODULE_ID), patch.object(module, "_secret_value", return_value="configured"):
                analyzer = module.ModerationAnalyzer()
                analyzer._call_openrouter = AsyncMock(return_value="негатив")

                for text in FALSE_POSITIVES:
                    self.assertEqual(asyncio.run(analyzer.analyze_tg(text)), "ок")
                self.assertEqual(asyncio.run(analyzer.analyze_tg("Вы тупые идиоты")), "негатив")


if __name__ == "__main__":
    unittest.main()
