# README — Environnement de travail : pamela + diana

But
- Préparer un environnement de travail rapide pour la démo en récupérant la branche du fork Pamela nommée [dev](https://github.com/TeodoreAutuly/PAMELAonCloud_FrameWork/tree/dev) et la branche du fork Diana nommée [collaborative-editing](https://github.com/mathismqn/diana/tree/feature/collaborative-editing) et en construisant/publiant `pamela` localement puis en compilant et lançant `diana`.

Prérequis
- Java JDK compatible
- Git
- Gradle wrapper (fourni dans les projets) et PowerShell ou un shell compatible

Étapes

1) Récupérer le code de `pamela` et `diana`

Depuis le répertoire racine du workspace (où se trouvent les dossiers `pamela` et `diana`):

```powershell
# entrer dans pamela
cd pamela
# récupérer et basculer sur la branche dev
git fetch origin
git checkout dev
git pull origin dev

# revenir et faire la même chose pour diana
cd ..\diana
git fetch origin
git checkout collaborative-editing
git pull origin collaborative-editing
```

2) Compiler et publier `pamela` localement

Depuis `pamela` (ou depuis la racine si vous préférez appeler le wrapper depuis là) :

```powershell
# compiler pamela core sans exécuter les tests
./gradlew :pamela-core:build -x test
# publier dans le dépôt Maven local
./gradlew :pamela-core:publishToMavenLocal -x test
```

Sur Windows PowerShell vous pouvez remplacer `./gradlew` par `./gradlew.bat` ou `.\gradlew.bat` si nécessaire.

3) Compiler et lancer `diana`

Depuis le dossier `diana` :

```powershell
# compiler le module diana drawing editor (rafraîchir les dépendances)
./gradlew :diana-drawing-editor:build -x test --refresh-dependencies
# lancer l'application
./gradlew :diana-drawing-editor:run
```

Remarques
- Les options `-x test` permettent de sauter les tests pour gagner du temps lors de la démo.
- `publishToMavenLocal` rend l'artifact `pamela-core` disponible pour `diana` via le repository Maven local.