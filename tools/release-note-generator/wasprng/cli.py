import gitlab

import argparse


def main():
    parser = argparse.ArgumentParser(description='Wasp release note generator')
    parser.add_argument('-t','--token', metavar='token', help='The gitlab authentication token')
    parser.add_argument('-s','--sprint', metavar='sprint', help='The sprint to build notes for')

    args = parser.parse_args()

    gl = gitlab.Gitlab('https://gitlab.com', private_token=args.token)
    project = gl.projects.get('AgileFactory/Agile.Wasp2')
    mrs = project.mergerequests.list(state='merged',milestone=args.sprint,order_by='updated_at')

    for mr in mrs:
        print '# %s' % (mr.title)
        print ''
        print '[Merge request %d](%s)' % (mr.iid, mr.web_url)
        print ''
        print 'Updated at: %s' % (mr.updated_at)
        print ''
        print 'Branch: %s' % (mr.source_branch)
        print ''
        print 'Author: [%s](%s)' % (mr.author['name'], mr.author["web_url"])
        print ''
        print mr.description
        print ''
        