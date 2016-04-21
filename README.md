# Hop

Hop is a key/value store designed to meet the requirements of systems
software and applications at exascale. Most key/value stores, and
generally storage systems are designed as monolithic systems that
provide certain range of functionality. Hop is designed as set of
building blocks that can be assembled and extended based on the
particular requirements for each application that uses it. This
approach allows Hop to provide more flexible balance between
scalability, consistency, reliability and performance than the
existing storage systems. Hop allows per-key consistency and
reliability configurations. Hop allows groups of users/ranks to create
consistency domains that ensure consistency within the group while
relaxing the requirements (and improving the performance) for
consistency for clients outside of the group. Hop also uses a novel
approach of using different key names to access the same data in
different ways/formats. A paper describing the initial work on Hop was
published at ISC 2015.

# Release

This software has been approved for open source release and has been
assigned **LA-CC-16-9**.

# Copyright

Copyright (c) 2015, Los Alamos National Security, LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

