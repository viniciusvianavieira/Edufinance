
from asyncio import tasks
from unicodedata import name

from paramiko import Channel
import discord
import datetime

from discord.ext import commands

bot = commands.Bot("!")

@bot.event
async def on_ready(): #ligando o bot
    print(f"Estou on e roteando. Meu nome é:{bot.user}")

   

@bot.event
async def on_message(message):
    if message.author == bot.user: #caso seja a resposta do robo retorna nada
        return

    if "lua" in message.content:
        await message.channel.send(f"Vi que citou o Lua com til,esse nome é proibido de ser falado nesse chat, por favor {message.author.name} não cite-o mais!")
    
        await message.delete()

    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction,user):
    print(reaction.emoji) 

    if reaction.emoji == "👍":
        role = user.guild.get_role(955535745995661412)
        await user.add_roles(role)



@bot.command(name="on?")
async def send_hello(ctx):

    name = ctx.author.name #o nome do perfil do autor

    response = "Robô funcionando normalmente"

    await ctx.send(response)

bot.run('OTU1NTM0Mjg0NzAwMTQ3NzYz.YjjEpg.9ucavVG5ZneNjuxMWu0w_-hO9-g')
