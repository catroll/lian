"""
- 驼峰式命名法 CamelCase
  - 小驼峰式命名法 lower Camel case
  - 大驼峰式命名法 upper Camel case，又叫帕斯卡命名法 PascalCase
- 下划线命名法 UnderLineCase
"""

import logging

LOG = logging.getLogger(__name__)


def camel2underline(name):
    try:
        return name[0].lower() + (''.join((c if c.islower() else '_' + c.lower()) for c in name[1:]))
    except Exception as error:
        LOG.warning('underline2camel error: %s', error)
        return name


def underline2camel(name):
    try:
        return ''.join(word.capitalize() for word in name.split('_'))
    except Exception as error:
        LOG.warning('underline2camel error: %s', error)
        return name


def __test():
    cases = (
        ('ZfjiyKjif', 'zfjiy_kjif'),
    )
    for a, b in cases:
        assert camel2underline(a) == b
        assert underline2camel(b) == a


if __name__ == '__main__':
    __test()
