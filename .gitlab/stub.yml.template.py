"""
Render the stub that the GitLab project setting *CI/CD configuration file*
refers to. The stub includes the pipeline for the branch being built.
"""
from azul.template import (
    emit,
)

emit({
    'include': [
        {
            'local': '.gitlab/standalone.yml'
        }
    ]
})
