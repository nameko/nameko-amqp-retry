import sys

import pkg_resources

PY3 = sys.version_info >= (3, 0)
PY34 = PY3 and sys.version_info[1] == 4
NAMEKO3 = pkg_resources.get_distribution("nameko").version.split('.')[0] == '3'
