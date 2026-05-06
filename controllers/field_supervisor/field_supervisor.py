from controller import Supervisor
import json, os, tempfile

supervisor = Supervisor()
ts = int(supervisor.getBasicTimeStep())

# ── Récupération des nœuds Webots ────────────────────────────────────────────
fields = {i: supervisor.getFromDef(f'FIELD_{i}') for i in range(1, 5)}
plants = {
    fid: [supervisor.getFromDef(f'PLANT_{fid}_{p}') for p in range(1, 10)]
    for fid in range(1, 5)
}
robots = {i: supervisor.getFromDef(f'robot_field_{i}') for i in range(1, 5)}

# ── Couleurs du sol selon l'action ───────────────────────────────────────────
# fertilisation : sol orange
# plantation    : sol vert
# arrosage      : sol humide (terre sombre)
# vide/recolte  : sol marron standard
SOIL_COLORS = {
    'vide'          : [0.45, 0.35, 0.25],   # marron standard
    'arrosage'      : [0.15, 0.12, 0.10],   # terre humide sombre (spec demandée)
    'fertilisation' : [0.65, 0.38, 0.10],   # sol orangé
    'plantation'    : [0.28, 0.48, 0.18],   # sol vert travaillé
    'recolte'       : [0.45, 0.35, 0.25],   # marron (comme vide)
    'recolte_done'  : [0.45, 0.35, 0.25],   # marron (après récolte)
    'arrosage_init' : [0.15, 0.12, 0.10],   # identique arrosage
}

# ── Couleurs LED selon l'action ───────────────────────────────────────────────
# vide/croissance : vert
# arrosage        : bleu
# fertilisation   : orange
# plantation      : vert clair
# recolte         : jaune
# après récolte   : vert (redevient vert)
LED_COLORS = {
    'vide'          : [0.0, 1.0,  0.0],    # vert
    'arrosage'      : [0.0, 0.0,  1.0],    # bleu
    'fertilisation' : [1.0, 0.53, 0.0],    # orange
    'plantation'    : [0.0, 1.0,  0.53],   # vert clair
    'recolte'       : [1.0, 1.0,  0.0],    # jaune
    'recolte_done'  : [0.0, 1.0,  0.0],    # vert (récolte terminée)
}

# ── État interne par champ ───────────────────────────────────────────────────
last_action    = {i: None  for i in range(1, 5)}
last_phase     = {i: None  for i in range(1, 5)}

# Apparition progressive (arrosage initial)
appear_idx     = {i: 0     for i in range(1, 5)}   # prochain indice de plante à afficher
appear_timer   = {i: 0.0   for i in range(1, 5)}   # timer pour apparition

# Récolte progressive
harvest_idx    = {i: 0     for i in range(1, 5)}
harvest_timer  = {i: 0.0   for i in range(1, 5)}

print("[Supervisor] Démarrage de la ferme intelligente 4 champs...")

# ── Helpers ──────────────────────────────────────────────────────────────────

def set_soil_color(fid, action):
    node = fields.get(fid)
    if not node:
        return
    try:
        color = SOIL_COLORS.get(action, SOIL_COLORS['vide'])
        node.getField('children').getMFNode(0) \
            .getField('appearance').getSFNode() \
            .getField('baseColor').setSFColor(color)
    except Exception as e:
        print(f"[Supervisor] Sol {fid}: {e}")


def set_led_color(fid, action):
    node = robots.get(fid)
    if not node:
        return
    try:
        color = LED_COLORS.get(action, LED_COLORS['vide'])
        children = node.getField('children')
        for i in range(children.getCount()):
            child = children.getMFNode(i)
            if child.getTypeName() == 'LED':
                led_ch = child.getField('children')
                if led_ch.getCount() > 0:
                    app = led_ch.getMFNode(0).getField('appearance').getSFNode()
                    app.getField('baseColor').setSFColor(color)
                    app.getField('emissiveColor').setSFColor(color)
                break
    except Exception as e:
        print(f"[Supervisor] LED {fid}: {e}")


def set_plant_color(app, r, g, b):
    """
    Met a jour baseColor ET emissiveColor=0 sur une PBRAppearance.
    Sans reset de emissiveColor, la couleur emissive du .wbt (0.1 0.3 0)
    s'ajoute au rendu et fausse toutes les couleurs (tout parait jaunatre).
    """
    try:
        app.getField('baseColor').setSFColor([r, g, b])
        app.getField('emissiveColor').setSFColor([0.0, 0.0, 0.0])
    except:
        pass


def set_plant_height(plant, height):
    """
    Change la hauteur ET ajuste translation Y = height/2
    pour que la base de la plante reste posee sur le sol.
    Un Cylinder Webots est centre sur son pivot: sans cet ajustement
    la moitie serait sous le sol.
    """
    try:
        shape = plant.getField('children').getMFNode(0)
        geom  = shape.getField('geometry').getSFNode()
        geom.getField('height').setSFFloat(height)
        t = plant.getField('translation').getSFVec3f()
        # Dans ce world: [x_lateral, y_ground, z_vertical] -> seul t[2] change
        plant.getField('translation').setSFVec3f([t[0], t[1], height / 2.0])
    except:
        pass


def set_plant_visible(fid, plant_idx, visible, growth=0.0):
    """Rend une plante visible ou invisible."""
    plant = plants[fid][plant_idx] if plant_idx < len(plants[fid]) else None
    if not plant:
        return
    try:
        shape = plant.getField('children').getMFNode(0)
        app   = shape.getField('appearance').getSFNode()
        if not visible:
            set_plant_height(plant, 0.001)
        else:
            height = max(0.06, 0.06 + (growth / 100.0) * 0.60)
            set_plant_height(plant, height)
            set_plant_color(app, 0.3, 0.85, 0.25)  # vert jeune
    except:
        pass


def hide_all_plants(fid):
    """Cache toutes les plantes d'un champ."""
    for idx in range(9):
        set_plant_visible(fid, idx, False)


def update_plant_growth(fid, growth):
    """
    Met a jour hauteur et couleur de TOUTES les plantes visibles.
    Ajuste aussi la translation Y = height/2 pour garder la base au sol.
    """
    for idx in range(9):
        if idx >= appear_idx[fid]:
            break
        plant = plants[fid][idx] if idx < len(plants[fid]) else None
        if not plant:
            continue
        try:
            shape = plant.getField('children').getMFNode(0)
            app   = shape.getField('appearance').getSFNode()

            if growth <= 0:
                set_plant_height(plant, 0.001)
                continue

            height = 0.06 + (growth / 100.0) * 0.60
            set_plant_height(plant, height)

            if growth >= 90:
                set_plant_color(app, 0.92, 0.82, 0.04)   # jaune mur
            elif growth >= 60:
                set_plant_color(app, 0.18, 0.62, 0.14)   # vert fonce
            else:
                set_plant_color(app, 0.30, 0.85, 0.25)   # vert clair jeune
        except:
            pass


def appear_next_plant(fid, growth):
    """Fait apparaître la prochaine plante (arrosage initial)."""
    idx = appear_idx[fid]
    if idx < 9:
        set_plant_visible(fid, idx, True, growth)
        appear_idx[fid] += 1
        if appear_idx[fid] >= 9:
            print(f"[Supervisor] Champ {fid}: toutes les plantes sont apparues ✓")


def harvest_next_plant(fid):
    """Cache la prochaine plante (récolte progressive)."""
    idx = harvest_idx[fid]
    if idx < 9:
        set_plant_visible(fid, idx, False)
        harvest_idx[fid] += 1
        if harvest_idx[fid] >= 9:
            print(f"[Supervisor] Champ {fid}: récolte terminée ✓")


# ── Initialisation ────────────────────────────────────────────────────────────
for fid in range(1, 5):
    set_soil_color(fid, 'vide')
    set_led_color(fid, 'vide')
    hide_all_plants(fid)

def read_field_data(fid):
    path = os.path.join(tempfile.gettempdir(), f'farm_field_{fid}.json')
    for _ in range(3):
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            pass
    return None


# ── État interne de récolte par champ (indépendant de la phase robot) ────────
# On garde en mémoire si une récolte est "en cours côté supervisor"
# pour ne pas masquer les plantes avant que la récolte progressive soit finie.
recolte_en_cours = {i: False for i in range(1, 5)}


# ── Boucle principale ────────────────────────────────────────────────────────
while supervisor.step(ts) != -1:
    for fid in range(1, 5):
        info = read_field_data(fid)
        if info is None:
            continue
        try:
            action = info.get('robot_action', 'vide')
            growth = info.get('plant_growth_pct', 0.0)
            phase  = info.get('phase', 'sol_vide')

            action_changed = (action != last_action[fid])
            phase_changed  = (phase  != last_phase[fid])

            # ── Couleur sol et LED : suivent l'action ────────────────────────
            if action_changed:
                set_soil_color(fid, action)
                set_led_color(fid, action)
                last_action[fid] = action

                # Début de récolte détecté → armer la récolte progressive
                if action == 'recolte':
                    recolte_en_cours[fid] = True
                    harvest_idx[fid]      = 0
                    harvest_timer[fid]    = 0.0

            # ── Récolte progressive (continue même si phase est passée à sol_vide)
            # On s'arrête seulement quand toutes les plantes ont disparu.
            if recolte_en_cours[fid] and harvest_idx[fid] < 9:
                harvest_timer[fid] += ts / 1000.0
                if harvest_timer[fid] >= 0.8:
                    harvest_timer[fid] = 0.0
                    harvest_next_plant(fid)
                    if harvest_idx[fid] >= 9:
                        recolte_en_cours[fid] = False
                        print(f"[Supervisor] Champ {fid}: recolte progressive terminee")

            # ── Transitions de phase ──────────────────────────────────────────
            if phase_changed:
                last_phase[fid] = phase

                if phase == 'arrosage_initial':
                    # Nouvelle plantation : préparer l'apparition progressive
                    appear_idx[fid]   = 0
                    appear_timer[fid] = 0.0
                    # Sécurité : si une récolte était encore en cours, on termine
                    if recolte_en_cours[fid]:
                        hide_all_plants(fid)
                        recolte_en_cours[fid] = False

                elif phase == 'sol_vide' and not recolte_en_cours[fid]:
                    # Sol vide ET récolte terminée → s'assurer que tout est caché
                    # (cas nominal : récolte déjà finie progressivement)
                    if harvest_idx[fid] >= 9:
                        pass  # déjà propre
                    # Si on arrive ici sans avoir eu de récolte (restart, etc.)
                    elif last_phase[fid] not in ('en_recolte', 'mature'):
                        hide_all_plants(fid)
                        appear_idx[fid] = 0

            # ── Apparition progressive des plantes (arrosage initial) ─────────
            if phase == 'arrosage_initial' and appear_idx[fid] < 9:
                appear_timer[fid] += ts / 1000.0
                if appear_timer[fid] >= 0.9:
                    appear_timer[fid] = 0.0
                    appear_next_plant(fid, growth)
            elif phase != 'arrosage_initial':
                appear_timer[fid] = 0.0

            # ── Croissance des plantes visibles ───────────────────────────────
            if phase in ('en_croissance', 'mature'):
                update_plant_growth(fid, growth)

        except Exception as e:
            print(f"[Supervisor] Champ {fid} erreur: {e}")
