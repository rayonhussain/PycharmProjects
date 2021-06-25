import math
import random

import pygame
from pygame import mixer

# initializing pygame package
pygame.init()

# game screen dimension
screen = pygame.display.set_mode((800, 600))

# background
bg = pygame.image.load("BG1.jpg")

# background music
mixer.music.load("bg_wav2.wav")
mixer.music.play(-1)

# title and game logo
pygame.display.set_caption("Space Game")
gamelogo = pygame.image.load("gamelogo.png")
pygame.display.set_icon(gamelogo)

# Player and position dimensions
playerlogo = pygame.image.load("rocketshooter.png")
playerX = 400
playerY = 600
playerX_change = 0
playerY_change = 0

# Enemy and position dimensions
enemylogo = []
enemyX = []
enemyY = []
enemyX_change = []
enemyY_change = []
num_of_enemies = 7

for i in range(num_of_enemies):
    enemylogo.append(pygame.image.load("alien.png"))
    enemyX.append(random.randint(0, 735))
    enemyY.append(random.randint(0, 50))
    enemyX_change.append(0.4)
    enemyY_change.append(70)

# Bullet and position dimensions
bulletlogo = pygame.image.load("bullet.png")
bulletX = 0
bulletY = 600
bulletX_change = 0
bulletY_change = 1
bullet_state = "ready"

# Score
score_value = 0
font = pygame.font.Font('freesansbold.ttf', 32)
gameover_font = pygame.font.Font('freesansbold.ttf', 64)

textX = 10
textY = 10


def show_score(x, y):
    score = font.render("Score: " + str(score_value), True, (255, 0, 124))
    screen.blit(score, (x, y))


def game_over():
    gameover_text = gameover_font.render("Game Over", True, (255, 255, 255))
    screen.blit(gameover_text, (200, 250))


def player(x, y):
    screen.blit(playerlogo, (x, y))


def enemy(x, y, i):
    screen.blit(enemylogo[i], (x, y))


def fire_bullet(x, y):
    global bullet_state
    bullet_state = "fire"
    screen.blit(bulletlogo, (x + 16, y + 10))


def isCollision(enemyX, enemyY, bulletX, bulletY):
    dist = math.sqrt(math.pow(enemyX - bulletX, 2) + math.pow(enemyY - bulletY, 2))
    if dist < 27:
        return True
    else:
        return False


# for window manual close loop
screen_window = True

while screen_window:
    # within for loop for Background (R,G,B)
    screen.fill((0, 0, 0))
    screen.blit(bg, (0, 0))

    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            screen_window = False

        # movement of player position
        if event.type == pygame.KEYDOWN:
            if event.key == pygame.K_LEFT:
                playerX_change = -0.4
            if event.key == pygame.K_RIGHT:
                playerX_change = 0.4
            if event.key == pygame.K_UP:
                playerY_change = -0.4
            if event.key == pygame.K_DOWN:
                playerY_change = 0.4
            if event.key == pygame.K_SPACE:
                if bullet_state == "ready":
                    bullet_sound = mixer.Sound("laser.wav")
                    bullet_sound.play()
                    bulletX = playerX
                    bulletY = playerY
                    fire_bullet(bulletX, bulletY)

        if event.type == pygame.KEYUP:
            if event.key == pygame.K_LEFT or event.key == pygame.K_RIGHT:
                playerX_change = 0
            if event.key == pygame.K_UP or event.key == pygame.K_DOWN:
                playerY_change = 0

    # player boundaries
    playerX += playerX_change
    if playerX <= 0:
        playerX = 0
    elif playerX >= 736:
        playerX = 736
    playerY += playerY_change
    if playerY >= 536:
        playerY = 536
    elif playerY <= 320:
        playerY = 320

    # enemy boundaries
    for i in range(num_of_enemies):
        # Game over text
        if enemyY[i] > 500:
            for j in range(num_of_enemies):
                enemyY[j] = 2000
            game_over()
            break

        enemyX[i] += enemyX_change[i]
        if enemyX[i] <= 0:
            enemyX_change[i] = 0.4
            enemyY[i] += enemyY_change[i]
        elif enemyX[i] >= 736:
            enemyX_change[i] = -0.4
            enemyY[i] += enemyY_change[i]

        # collision
        collision = isCollision(enemyX[i], enemyY[i], bulletX, bulletY)
        if collision:
            collision_sound = mixer.Sound("explosion.wav")
            collision_sound.play()
            bulletY = 600
            bullet_state = "ready"
            score_value += 1
            enemyX[i] = random.randint(0, 735)
            enemyY[i] = random.randint(0, 50)

        enemy(enemyX[i], enemyY[i], i)

    # bullet movement
    if bulletY <= 0:
        bulletY = 600
        bullet_state = "ready"

    if bullet_state == "fire":
        fire_bullet(bulletX, bulletY)
        bulletY -= bulletY_change

    # call player function after fill since it will disappear if its first
    player(playerX, playerY)
    show_score(textX, textY)
    # need to update game screen when made changes
    pygame.display.update()
