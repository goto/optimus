"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[2397],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>d});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},c="mdxType",m={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},g=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,s=e.parentName,u=l(e,["components","mdxType","originalType","parentName"]),c=p(n),g=a,d=c["".concat(s,".").concat(g)]||c[g]||m[g]||i;return n?r.createElement(d,o(o({ref:t},u),{},{components:n})):r.createElement(d,o({ref:t},u))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=g;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[c]="string"==typeof e?e:a,o[1]=l;for(var p=2;p<i;p++)o[p]=n[p];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}g.displayName="MDXCreateElement"},8577:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>m,frontMatter:()=>i,metadata:()=>l,toc:()=>p});var r=n(7462),a=(n(7294),n(3905));const i={},o="Starting Optimus Server",l={unversionedId:"server-guide/starting-optimus-server",id:"server-guide/starting-optimus-server",title:"Starting Optimus Server",description:"Starting a server requires server configuration, which can be loaded from file (use --config flag), environment",source:"@site/docs/server-guide/starting-optimus-server.md",sourceDirName:"server-guide",slug:"/server-guide/starting-optimus-server",permalink:"/optimus/docs/server-guide/starting-optimus-server",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/server-guide/starting-optimus-server.md",tags:[],version:"current",lastUpdatedBy:"Anwar Hidayat",lastUpdatedAt:1701161021,formattedLastUpdatedAt:"Nov 28, 2023",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Installing Plugins",permalink:"/optimus/docs/server-guide/installing-plugins"},next:{title:"DB Migrations",permalink:"/optimus/docs/server-guide/db-migrations"}},s={},p=[],u={toc:p},c="wrapper";function m(e){let{components:t,...n}=e;return(0,a.kt)(c,(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"starting-optimus-server"},"Starting Optimus Server"),(0,a.kt)("p",null,"Starting a server requires server configuration, which can be loaded from file (use --config flag), environment\nvariable ",(0,a.kt)("inlineCode",{parentName:"p"},"OPTIMUS_[CONFIGNAME]"),", or config.yaml file in Optimus binary directory."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"1. Using --config flag")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus serve --config /path/to/config/file.yaml\n")),(0,a.kt)("p",null,"If you specify the configuration file using the --config flag, then any configs defined in the env variable and default\nconfig.yaml from the Optimus binary directory will not be loaded."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"2. Using environment variable")),(0,a.kt)("p",null,"All the configs can be passed as environment variables using ",(0,a.kt)("inlineCode",{parentName:"p"},"OPTIMUS_[CONFIG_NAME]")," convention. ","[CONFIG_NAME]"," is the\nkey name of config using ",(0,a.kt)("inlineCode",{parentName:"p"},"_")," as the path delimiter to concatenate between keys."),(0,a.kt)("p",null,"For example, to use the environment variable, assuming the following configuration layout:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-yaml"},"version: 1\nserve:\n  port: 9100\n  app_key: randomhash\n")),(0,a.kt)("p",null,"Here is the corresponding environment variable for the above"),(0,a.kt)("table",null,(0,a.kt)("thead",{parentName:"table"},(0,a.kt)("tr",{parentName:"thead"},(0,a.kt)("th",{parentName:"tr",align:null},"Configuration key"),(0,a.kt)("th",{parentName:"tr",align:null},"Environment variable"))),(0,a.kt)("tbody",{parentName:"table"},(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"version"),(0,a.kt)("td",{parentName:"tr",align:null},"OPTIMUS_VERSION")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"serve.port"),(0,a.kt)("td",{parentName:"tr",align:null},"OPTIMUS_PORT")),(0,a.kt)("tr",{parentName:"tbody"},(0,a.kt)("td",{parentName:"tr",align:null},"serve.app_key"),(0,a.kt)("td",{parentName:"tr",align:null},"OPTIMUS_SERVE_APP_KEY")))),(0,a.kt)("p",null,"Set the env variable using export"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ export OPTIMUS_PORT=9100\n")),(0,a.kt)("p",null,"Note: If you specify the env variable and you also have config.yaml in the Optimus binary directory, then any configs\nfrom the env variable will override the configs defined in config.yaml in Optimus binary directory."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"3. Using default config.yaml from Optimus binary directory")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"$ which optimus\n/usr/local/bin/optimus\n")),(0,a.kt)("p",null,"So the config.yaml file can be loaded on /usr/local/bin/config.yaml"))}m.isMDXComponent=!0}}]);