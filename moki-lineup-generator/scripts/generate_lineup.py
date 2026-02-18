
import json
import csv
import random

def calculate_champion_score(champion_stats):
    """Calculates the score for a single champion based on their stats."""
    winrate = float(champion_stats['winrate'].strip('%'))
    avg_elims = float(champion_stats['avg elims'])
    avg_balls = float(champion_stats['avg balls'])
    avg_wart = float(champion_stats['avg wart'])

    elimination_score = avg_elims * 80
    ball_score = avg_balls * 50
    wart_score = (avg_wart / 80) * 45
    victory_score = (winrate / 100) * 300

    total_score = elimination_score + ball_score + wart_score + victory_score
    return total_score

def generate_lineup():
    """Generates a Moki Grand Arena lineup."""
    try:
        with open('../../champions.json', 'r') as f:
            champions = json.load(f)
    except FileNotFoundError:
        print("Error: champions.json not found. Make sure you are in the project root directory.")
        return

    try:
        with open('../../game.csv', 'r') as f:
            reader = csv.DictReader(f)
            game_stats = list(reader)
    except FileNotFoundError:
        print("Error: game.csv not found. Make sure you are in the project root directory.")
        return

    try:
        with open('../../schemes.json', 'r') as f:
            schemes = json.load(f)
    except FileNotFoundError:
        print("Error: schemes.json not found. Make sure you are in the project root directory.")
        return

    champion_scores = []
    for stats in game_stats:
        score = calculate_champion_score(stats)
        champion_scores.append({'name': stats['name'], 'score': score})

    sorted_champions = sorted(champion_scores, key=lambda x: x['score'], reverse=True)

    top_4_champions = sorted_champions[:4]

    random_scheme = random.choice(schemes)

    print("Recommended Moki Grand Arena Lineup:")
    print("====================================")
    print("
Champions:")
    for i, champion in enumerate(top_4_champions):
        print(f"  {i+1}. {champion['name']} (Score: {champion['score']:.2f})")

    print("
Scheme Card:")
    print(f"  - {random_scheme['name']}: {random_scheme['description']}")
    print("
====================================")

if __name__ == '__main__':
    generate_lineup()
