import json
import csv

from itertools import combinations

def get_champion_traits(champions):
    """Creates a mapping from champion name to traits."""
    traits = {}
    for champion in champions:
        traits[champion['name']] = champion['traits']
    return traits

def _has_matching_trait(champion_traits, trait_criteria):
    """
    Checks if a champion has a matching trait based on the provided criteria.
    Criteria can be a list of exact traits or a dictionary with 'contains' keywords.
    """
    if isinstance(trait_criteria, list): # Exact match
        for trait in champion_traits:
            if trait in trait_criteria:
                return True
    elif isinstance(trait_criteria, dict) and "contains" in trait_criteria: # Contains keyword
        keywords = trait_criteria["contains"]
        for champ_trait in champion_traits:
            for keyword in keywords:
                if keyword in champ_trait:
                    return True
    return False


def calculate_champion_score_base(champion_stats):
    """Calculates the base score for a single champion without scheme modifications."""
    winrate = float(champion_stats['winrate'].strip('%'))
    avg_elims = float(champion_stats['avg elims'])
    avg_balls = float(champion_stats['avg balls'])
    avg_wart = float(champion_stats['avg wart'])

    elimination_score = avg_elims * 80
    ball_score = avg_balls * 50
    wart_score = (avg_wart / 80) * 45
    victory_score = (winrate / 100) * 300

    return elimination_score + ball_score + wart_score + victory_score

def calculate_scheme_effects(champion_stats, base_score_components, scheme):
    """Calculates the scheme-specific bonus for a champion (non-trait)."""
    scheme_bonus = 0
    
    winrate = float(champion_stats['winrate'].strip('%'))
    elimination_score = base_score_components['elimination']
    ball_score = base_score_components['ball']
    wart_score = base_score_components['wart']
    victory_score = base_score_components['victory']

    scheme_name = scheme['name']
    avg_elims = float(champion_stats['avg elims'])
    avg_balls = float(champion_stats['avg balls'])


    if scheme_name == "Aggressive Specialization":
        scheme_bonus += (elimination_score * 0.5)
        scheme_bonus -= ball_score
        scheme_bonus -= wart_score
    elif scheme_name == "Collective Specialization":
        scheme_bonus += (ball_score * 0.4)
        scheme_bonus -= elimination_score
        scheme_bonus -= wart_score
    elif scheme_name == "Victory Lap":
        scheme_bonus += 100 * (winrate / 100)
    elif scheme_name == "Taking a Dive":
        scheme_bonus += 150 * (1 - (winrate / 100))
    elif scheme_name == "Gacha Gouging":
        scheme_bonus += avg_balls * 15
        scheme_bonus += avg_elims * 20
    elif scheme_name == "Cage Match":
        scheme_bonus += avg_elims * 35
        scheme_bonus += avg_balls * 10
    
    return scheme_bonus

def format_lineups_to_markdown(lineups):
    """Formats the sorted lineups into a markdown string."""
    output = "# Moki Grand Arena Lineups\n\n"
    for lineup in lineups:
        output += "## Best lineup for scheme: " + lineup['scheme_name'] + "\n"
        output += "**Description:** " + lineup['scheme_description'] + "\n\n"

        if not lineup['supported']:
            output += "> This scheme is not yet supported for lineup optimization.\n\n"
            continue
        
        output += "**Champions:**\n\n"
        for i, champion in enumerate(lineup['champions']):
            if champion['scheme_bonus'] != 0:
                output += f"1. **{champion['name']}** (Score: {champion['base_score']:.2f} + Bonus {champion['scheme_bonus']:.2f})\n"
            else:
                output += f"1. **{champion['name']}** (Score: {champion['base_score']:.2f})\n"
        
        output += "\n"

        if lineup['total_scheme_bonus'] != 0:
            output += f"**Total Scheme Bonus for Lineup:** {lineup['total_scheme_bonus']:.2f}\n"

        output += f"**Total Lineup Score:** {lineup['total_lineup_score']:.2f}\n\n"
        output += "---\n\n"
    return output


def generate_lineup():
    """Generates a Moki Grand Arena lineup for each scheme card."""
    try:
        with open('champions.json', 'r') as f:
            champions_data = json.load(f)
    except FileNotFoundError:
        print("Error: champions.json not found. Make sure you are in the project root directory.")
        return

    try:
        with open('game.csv', 'r') as f:
            reader = csv.DictReader(f)
            game_stats_data = list(reader)
    except FileNotFoundError:
        print("Error: game.csv not found. Make sure you are in the project root directory.")
        return

    try:
        with open('schemes.json', 'r') as f:
            schemes_data = json.load(f)
    except FileNotFoundError:
        print("Error: schemes.json not found. Make sure you are in the project root directory.")
        return

    champion_traits_map = get_champion_traits(champions_data)

    supported_schemes = {
        "Aggressive Specialization": None,
        "Collective Specialization": None,
        "Victory Lap": None,
        "Taking a Dive": None,
        "Gacha Gouging": None,
        "Cage Match": None,
        "Costume Party": {"contains": ["Onesie", "Lemon Head", "Kappa Head", "Tomato Head", "Bear Head", "Frog Head", "Blob Head"]},
        "Divine Intervention": {"contains": ["Spirit"]},
        "Shapeshifting": {"contains": ["Tongue out", "Tanuki mask", "Kitsune Mask", "Cat Mask"]},
        "Midnight Strike": {"contains": ["Shadow"]},
        "Dress to Impress": {"contains": ["Kimono"]},
        "Housekeeping": {"contains": ["Apron", "Garbage Can", "Toilet Paper"]},
        "Dungaree Duel": {"contains": ["Overalls"]},
        "Tear Jerking": {"contains": ["Crying"]},
        "Golden Shower": ["Gold"],
        "Rainbow Riot": {"contains": ["Rainbow"]},
        "Whale Watching": {"contains": ["1 of 1"]},
        "Call to Arms": {"contains": ["Ronin", "Samurai"]},
        "Malicious Intent": {"contains": ["Devious", "Oni", "Tengu", "Skull Mask"]}
    }

    trait_based_schemes = [
        "Costume Party", "Divine Intervention", "Shapeshifting", "Midnight Strike",
        "Dress to Impress", "Housekeeping", "Dungaree Duel", "Tear Jerking",
        "Golden Shower", "Rainbow Riot", "Whale Watching", "Call to Arms", "Malicious Intent"
    ]

    all_generated_lineups = []

    for scheme in schemes_data:
        
        lineup_data = {
            "scheme_name": scheme['name'],
            "scheme_description": scheme['description'],
            "supported": True,
            "champions": [],
            "total_scheme_bonus": 0,
            "total_lineup_score": 0
        }

        if scheme['name'] not in supported_schemes:
            lineup_data["supported"] = False
            all_generated_lineups.append(lineup_data)
            continue

        all_champion_scores = []
        for stats in game_stats_data:
            winrate = float(stats['winrate'].strip('%'))
            base_score_components = {
                'elimination': float(stats['avg elims']) * 80,
                'ball': float(stats['avg balls']) * 50,
                'wart': (float(stats['avg wart']) / 80) * 45,
                'victory': (winrate / 100) * 300
            }
            base_score = sum(base_score_components.values())
            scheme_bonus = calculate_scheme_effects(stats, base_score_components, scheme)
            
            all_champion_scores.append({
                'name': stats['name'],
                'score': base_score + scheme_bonus,
                'base_score': base_score,
                'scheme_bonus': scheme_bonus,
                'traits': champion_traits_map.get(stats['name'], [])
            })
        
        top_4_champions_final = []
        if scheme['name'] in trait_based_schemes:
            trait_criteria = supported_schemes[scheme['name']]
            champions_with_trait = sorted([c for c in all_champion_scores if _has_matching_trait(c['traits'], trait_criteria)], key=lambda x: x['score'], reverse=True)
            champions_without_trait = sorted([c for c in all_champion_scores if not _has_matching_trait(c['traits'], trait_criteria)], key=lambda x: x['score'], reverse=True)

            best_lineup = []
            max_lineup_score = -1

            for num_trait_champs in range(5):
                num_without_trait = 4 - num_trait_champs
                if len(champions_with_trait) >= num_trait_champs and len(champions_without_trait) >= num_without_trait:
                    lineup_combo = champions_with_trait[:num_trait_champs] + champions_without_trait[:num_without_trait]
                    
                    lineup_trait_base_bonus = num_trait_champs * 25
                    
                    current_lineup_score = 0
                    for champion in lineup_combo:
                        champion_final_score = champion['score']
                        if _has_matching_trait(champion['traits'], trait_criteria):
                            champion_final_score += lineup_trait_base_bonus
                        current_lineup_score += champion_final_score
                    
                    if current_lineup_score > max_lineup_score:
                        max_lineup_score = current_lineup_score
                        best_lineup = lineup_combo

            final_best_lineup = []
            if best_lineup:
                num_trait_champs_in_best = sum(1 for c in best_lineup if _has_matching_trait(c['traits'], trait_criteria))
                lineup_trait_bonus = num_trait_champs_in_best * 25

                for champion in best_lineup:
                    final_champ = dict(champion)
                    if _has_matching_trait(final_champ['traits'], trait_criteria):
                        final_champ['scheme_bonus'] += lineup_trait_bonus
                        final_champ['score'] += lineup_trait_bonus
                    final_best_lineup.append(final_champ)

            top_4_champions_final = sorted(final_best_lineup, key=lambda x: x['score'], reverse=True)

        else:
             top_4_champions_final = sorted(all_champion_scores, key=lambda x: x['score'], reverse=True)[:4]

        lineup_data["champions"] = top_4_champions_final
        lineup_data["total_scheme_bonus"] = sum(c['scheme_bonus'] for c in top_4_champions_final)
        lineup_data["total_lineup_score"] = sum(c['score'] for c in top_4_champions_final)
        all_generated_lineups.append(lineup_data)

    
    sorted_lineups = sorted(all_generated_lineups, key=lambda x: x['total_lineup_score'], reverse=True)
    
    markdown_output = format_lineups_to_markdown(sorted_lineups)
    
    try:
        with open('moki_lineups.md', 'w') as f:
            f.write(markdown_output)
        print("Successfully created moki_lineups.md")
    except IOError as e:
        print(f"Error writing to moki_lineups.md: {e}")


if __name__ == '__main__':
    generate_lineup()
