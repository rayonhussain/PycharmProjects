import pygame
from pygame import mixer

pygame.init()

screen = pygame.display.set_mode((800, 600))
bg = pygame.image.load("")

pygame.display.set_caption("")
gamelogo = pygame.image.load("")
pygame.display.set_icon(gamelogo)

playerlogo = pygame.image.load("")
playerX = 400
playerY = 600
playerX_change = 0
playerY_change = 0

mixer.music.load("")
mixer.music.play(-1)

screen_window = True

while screen_window:
    screen.fill((0, 0, 0))
    screen.blit(bg, (0, 0))
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            screen_window = False

    pygame.display.update()
