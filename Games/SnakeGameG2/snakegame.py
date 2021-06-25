import pygame
import random
import math
from pygame import mixer

pygame.init()

#screen setting
screen = pygame.display.set_mode((800,600))
bg = pygame.image.load("bg1.jpg")

#snake game logo and caption
pygame.display.set_caption("Snake Game")
gamelogo = pygame.image.load("snake.png")
pygame.display.set_icon(gamelogo)

#icons in game
applelogo=pygame.image.load("apple.jpg")
appleX = random.randint(0, 760)
appleY = random.randint(0, 560)
blocklogo=pygame.image.load("block.jpg")

#random.randint(0, 760)
#random.randint(0, 560)
blockX =100
blockY =100
blockX_change=0
blockY_change=0

score=0
dist=0
x=random.randint(0,760)
y=random.randint(0,560)
apple_status="ready"

#bg music
mixer.music.load("bgmusic.mp3")
mixer.music.play(-1)

#functions
def block(x,y):
    screen.blit(blocklogo, (blockX, blockY))

def apple(x,y):
    screen.blit(applelogo, (appleX, appleY))

def isCollision(appleX,appleY,blockX,blockY):
    dist=math.sqrt(math.pow(appleX-blockX,2)+math.pow(appleY-blockY,2))
    if dist < 30:
        return True
    else:
        return False

#variables
screen_window = True


while screen_window:
    screen.fill((255, 255, 255))
    screen.blit(bg, (0, 0))



    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            screen_window = False

        if event.type == pygame.KEYDOWN:
            if event.key == pygame.K_LEFT:
                blockY_change = 0
                blockX_change = -0.4
            if event.key == pygame.K_RIGHT:
                blockY_change = 0
                blockX_change = 0.4
            if event.key == pygame.K_UP:
                blockX_change = 0
                blockY_change = -0.4
            if event.key == pygame.K_DOWN:
                blockX_change = 0
                blockY_change = 0.4



    if blockX > 800:
        blockX = 0
    elif blockX < -40:
        blockX = 800
    if blockY > 600:
        blockY = 0
    elif blockY < -40:
        blockY = 600
    blockX += blockX_change
    blockY += blockY_change
    block(blockX, blockY)

    apple_status="ready"
    apple(appleX, appleY)
    collision = isCollision(appleX, appleY, blockX, blockY)
    if collision:
        score += 1
        print(score)
        apple_status="eaten"

    if apple_status=="eaten":
        appleX = random.randint(0, 760)
        appleY = random.randint(0, 560)

    pygame.display.update()