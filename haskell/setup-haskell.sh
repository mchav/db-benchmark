#!/bin/bash
set -e

# Install Stack (Haskell build tool) if not present
if ! command -v stack &> /dev/null; then
    echo "Installing Stack..."
    curl -sSL https://get.haskellstack.org/ | sh
fi

cd haskell

# Initialize stack project if not already done
if [ ! -f "stack.yaml" ]; then
    stack init --force
fi

# Create cabal file if it doesn't exist
if [ ! -f "haskell-benchmark.cabal" ]; then
    cat > haskell-benchmark.cabal << 'EOF'
name:                haskell-benchmark
version:             0.1.0.0
build-type:          Simple
cabal-version:       >=1.10

executable groupby-haskell
  main-is:             groupby-haskell.hs
  build-depends:       base >= 4.7 && < 5
                     , Frames >= 0.7
                     , vinyl >= 0.13
                     , text >= 1.2
                     , bytestring >= 0.10
                     , vector >= 0.12
                     , cassava >= 0.5
                     , pipes >= 4.3
                     , time >= 1.9
                     , process >= 1.6
                     , directory >= 1.3
                     , containers >= 0.6
                     , hashable >= 1.3
                     , unordered-containers >= 0.2
  default-language:    Haskell2010
  ghc-options:         -O2 -threaded -rtsopts -with-rtsopts=-N

executable join-haskell
  main-is:             join-haskell.hs
  build-depends:       base >= 4.7 && < 5
                     , Frames >= 0.7
                     , vinyl >= 0.13
                     , text >= 1.2
                     , bytestring >= 0.10
                     , vector >= 0.12
                     , cassava >= 0.5
                     , pipes >= 4.3
                     , time >= 1.9
                     , process >= 1.6
                     , directory >= 1.3
                     , containers >= 0.6
                     , hashable >= 1.3
                     , unordered-containers >= 0.2
  default-language:    Haskell2010
  ghc-options:         -O2 -threaded -rtsopts -with-rtsopts=-N
EOF
fi

# Install dependencies and build
stack setup
stack build --only-dependencies
stack build

cd ..

./haskell/ver-haskell.sh
