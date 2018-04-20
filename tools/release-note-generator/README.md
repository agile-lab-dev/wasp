### Create new Sprint Release Notes

*Optional: `virtualenv ~/release-note-generator-virtual-env`*

1. `source ~/release-note-generator-virtual-env/bin/activate`

2. `cd ~/Projects/Agile.Wasp2/tools/release-note-generator`

3. `python setup.py install`

4. `wasp-release-note-generator -t <USER_GITLAB_ACCESS_TOKEN> --sprint "<MILESTONE>" > ~/Desktop/newRelease.md`

5. `deactivate`