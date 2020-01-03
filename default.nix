{ compiler    ? "ghc881"
, doBenchmark ? false
, doTracing   ? false
, doStrict    ? false
, rev         ? "ec29bb50bf45531b932670c9da743f044a546ed5"
, sha256      ? "0anb25h7b9r8rzwkkhi51g59x27xacg9k3agf9lnzzx44lsw89jh"
, pkgs        ? import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    inherit sha256; }) {
    config.allowUnfree = true;
    config.allowBroken = false;
  }
, returnShellEnv ? pkgs.lib.inNixShell
, mkDerivation ? null
}:

let haskellPackages = pkgs.haskell.packages.${compiler};

in haskellPackages.developPackage {
  root = ./.;

  source-overrides = {
  };

  modifier = drv: pkgs.haskell.lib.overrideCabal drv (attrs: {
    inherit doBenchmark;
  });

  inherit returnShellEnv;
}
