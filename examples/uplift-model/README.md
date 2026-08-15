# Example: uplift-model

Minimal example of a downstream repo that registers a `ModelSpec` on tag push.

Local dry-run:

    causalops register \
        --spec-path model_spec.py \
        --git-repo local/uplift-model \
        --git-tag v3.1.0 \
        --git-sha $(git rev-parse HEAD) \
        --registered-by "$USER"
