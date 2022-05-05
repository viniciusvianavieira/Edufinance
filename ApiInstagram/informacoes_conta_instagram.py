from informacoes_iniciais_conta_instagram import informacoes_iniciais_conta_instagram
from informacoes_backup_conta_instagram import informacoes_backup_conta_instagram
import os


os.system('cls' if os.name == 'nt' else 'clear')
print()
informacoes_iniciais_conta_instagram.pegando_informacoes_iniciais()
informacoes_backup_conta_instagram.pegando_informacoes_backup()
