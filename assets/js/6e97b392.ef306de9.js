"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[3390],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>m});var r=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function s(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?s(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):s(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,i=function(e,t){if(null==e)return{};var n,r,i={},s=Object.keys(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var a=r.createContext({}),u=function(e){var t=r.useContext(a),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=u(e.components);return r.createElement(a.Provider,{value:t},e.children)},c="mdxType",g={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,i=e.mdxType,s=e.originalType,a=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=u(n),d=i,m=c["".concat(a,".").concat(d)]||c[d]||g[d]||s;return n?r.createElement(m,o(o({ref:t},p),{},{components:n})):r.createElement(m,o({ref:t},p))}));function m(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var s=n.length,o=new Array(s);o[0]=d;var l={};for(var a in t)hasOwnProperty.call(t,a)&&(l[a]=t[a]);l.originalType=e,l[c]="string"==typeof e?e:i,o[1]=l;for(var u=2;u<s;u++)o[u]=n[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},8175:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>a,contentTitle:()=>o,default:()=>g,frontMatter:()=>s,metadata:()=>l,toc:()=>u});var r=n(7462),i=(n(7294),n(3905));const s={},o="Installing Plugins",l={unversionedId:"server-guide/installing-plugins",id:"server-guide/installing-plugins",title:"Installing Plugins",description:"Plugin needs to be installed in the Optimus server before it can be used. Optimus uses the following directories for",source:"@site/docs/server-guide/installing-plugins.md",sourceDirName:"server-guide",slug:"/server-guide/installing-plugins",permalink:"/optimus/docs/server-guide/installing-plugins",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/server-guide/installing-plugins.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1697524450,formattedLastUpdatedAt:"Oct 17, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Server Configuration",permalink:"/optimus/docs/server-guide/configuration"},next:{title:"Starting Optimus Server",permalink:"/optimus/docs/server-guide/starting-optimus-server"}},a={},u=[],p={toc:u},c="wrapper";function g(e){let{components:t,...n}=e;return(0,i.kt)(c,(0,r.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"installing-plugins"},"Installing Plugins"),(0,i.kt)("p",null,"Plugin needs to be installed in the Optimus server before it can be used. Optimus uses the following directories for\ndiscovering plugin binaries"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"./.plugins\n./\n<exec>/\n<exec>/.optimus/plugins\n$HOME/.optimus/plugins\n/usr/bin\n/usr/local/bin\n")),(0,i.kt)("p",null,"Even though the above list of directories is involved in plugin discovery, it is advised to use .plugins in the\ncurrent working directory of the project or Optimus binary."),(0,i.kt)("p",null,"To simplify installation, you can add plugin artifacts in the server config:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-yaml"},"plugin:\n  artifacts:\n   - https://...path/to/optimus-plugin-neo.yaml  # http\n   - http://.../plugins.zip # zip\n   - ../transformers/optimus-bq2bq_darwin_arm64 # relative paths\n   - ../transformers/optimus-plugin-neo.yaml\n")),(0,i.kt)("p",null,"Run below command to auto-install the plugins in the ",(0,i.kt)("inlineCode",{parentName:"p"},".plugins")," directory."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus plugin install -c config.yaml  # This will install plugins in the `.plugins` folder.\n")))}g.isMDXComponent=!0}}]);