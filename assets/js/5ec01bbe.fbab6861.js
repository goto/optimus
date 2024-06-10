"use strict";(self.webpackChunkoptimus=self.webpackChunkoptimus||[]).push([[4648],{3905:(e,t,n)=>{n.d(t,{Zo:()=>m,kt:()=>h});var a=n(7294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,i=function(e,t){if(null==e)return{};var n,a,i={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var r=a.createContext({}),u=function(e){var t=a.useContext(r),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},m=function(e){var t=u(e.components);return a.createElement(r.Provider,{value:t},e.children)},p="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,i=e.mdxType,o=e.originalType,r=e.parentName,m=s(e,["components","mdxType","originalType","parentName"]),p=u(n),d=i,h=p["".concat(r,".").concat(d)]||p[d]||c[d]||o;return n?a.createElement(h,l(l({ref:t},m),{},{components:n})):a.createElement(h,l({ref:t},m))}));function h(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var o=n.length,l=new Array(o);l[0]=d;var s={};for(var r in t)hasOwnProperty.call(t,r)&&(s[r]=t[r]);s.originalType=e,s[p]="string"==typeof e?e:i,l[1]=s;for(var u=2;u<o;u++)l[u]=n[u];return a.createElement.apply(null,l)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},1957:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>r,contentTitle:()=>l,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>u});var a=n(7462),i=(n(7294),n(3905));const o={},l="Work with Extension",s={unversionedId:"client-guide/work-with-extension",id:"client-guide/work-with-extension",title:"Work with Extension",description:"Extension helps you to include third-party or arbitrary implementation as part of Optimus. Currently, the extension is",source:"@site/docs/client-guide/work-with-extension.md",sourceDirName:"client-guide",slug:"/client-guide/work-with-extension",permalink:"/optimus/docs/client-guide/work-with-extension",draft:!1,editUrl:"https://github.com/goto/optimus/edit/master/docs/docs/client-guide/work-with-extension.md",tags:[],version:"current",lastUpdatedBy:"Yash Bhardwaj",lastUpdatedAt:1718021158,formattedLastUpdatedAt:"Jun 10, 2024",frontMatter:{},sidebar:"docsSidebar",previous:{title:"Replay a Job (Backfill)",permalink:"/optimus/docs/client-guide/replay-a-job"},next:{title:"Defining Scheduler Version",permalink:"/optimus/docs/client-guide/defining-scheduler-version"}},r={},u=[{value:"Warning",id:"warning",level:2},{value:"Limitation",id:"limitation",level:2},{value:"Creating",id:"creating",level:2},{value:"Commands",id:"commands",level:2},{value:"Installation",id:"installation",level:3},{value:"Executing",id:"executing",level:3},{value:"Activate",id:"activate",level:3},{value:"Describe",id:"describe",level:3},{value:"Rename",id:"rename",level:3},{value:"Uninstall",id:"uninstall",level:3},{value:"Upgrade",id:"upgrade",level:3}],m={toc:u},p="wrapper";function c(e){let{components:t,...n}=e;return(0,i.kt)(p,(0,a.Z)({},m,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"work-with-extension"},"Work with Extension"),(0,i.kt)("p",null,"Extension helps you to include third-party or arbitrary implementation as part of Optimus. Currently, the extension is\ndesigned for when the user is running it as CLI."),(0,i.kt)("h2",{id:"warning"},"Warning"),(0,i.kt)("p",null,"Extension is basically an executable file outside Optimus. ",(0,i.kt)("em",{parentName:"p"},"We do not guarantee whether an extension is safe or not"),".\nWe suggest checking the extension itself, whether it is safe to run in your local or not, before installing and running it."),(0,i.kt)("h2",{id:"limitation"},"Limitation"),(0,i.kt)("p",null,"Extension is designed to be similar to ",(0,i.kt)("a",{parentName:"p",href:"https://cli.github.com/manual/gh_extension"},"GitHub extension"),". However, since\nit's still in the early stage, some limitations are there."),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"extension is only an executable file"),(0,i.kt)("li",{parentName:"ul"},"installation only looks at the GitHub asset according to the running system OS and Architecture"),(0,i.kt)("li",{parentName:"ul"},"convention for extension:",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"extension repository should follow optimus-extension-","[name of extension]"," (example: ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/gojek/optimus-extension-valor"},"optimus-extension-valor"),")"),(0,i.kt)("li",{parentName:"ul"},"asset being considered is binary with suffix ...","[OS]","-","[ARC]"," (example: when installing valor, if the user's OS is "),(0,i.kt)("li",{parentName:"ul"},"Linux and the architecture is AMD64, then installation will consider valor_linux-amd64 as binary to be executed)")))),(0,i.kt)("h2",{id:"creating"},"Creating"),(0,i.kt)("p",null,"Extension is designed to be open. Anyone could create their own extension. And as long as it is available,\nanyone could install it. In order to create it, the following are the basic steps to do:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Decide the name of the extension, example: ",(0,i.kt)("em",{parentName:"li"},"valor")),(0,i.kt)("li",{parentName:"ul"},"Create a GitHub repository that follows the convention, example: ",(0,i.kt)("em",{parentName:"li"},"optimus-extension-valor")),(0,i.kt)("li",{parentName:"ul"},"Put some implementation and asset with the name based on the convention, example: ",(0,i.kt)("em",{parentName:"li"},"valor_linux-amd64, valor_darwin-amd64"),", and more."),(0,i.kt)("li",{parentName:"ul"},"Ensure it is available for anyone to download")),(0,i.kt)("h2",{id:"commands"},"Commands"),(0,i.kt)("p",null,"Optimus supports some commands to help operate on extension."),(0,i.kt)("h3",{id:"installation"},"Installation"),(0,i.kt)("p",null,"You can run the installation using Optimus sub-command install under the extension. In order to install an extension, run the following command:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension install REMOTE [flags]\n")),(0,i.kt)("p",null,"You can use the ",(0,i.kt)("em",{parentName:"p"},"--alias")," flag to change the command name, since by default, Optimus will try to figure it out by itself.\nAlthough, during this process, sometimes an extension name conflicts with the reserved commands. This flag helps to\nresolve that. But, do note that this flag cannot be used to rename an ",(0,i.kt)("em",{parentName:"p"},"installed")," extension. To do such a thing, check rename."),(0,i.kt)("p",null,"REMOTE is the Github remote path where to look for the extension. REMOTE can be in the form of"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"OWNER/PROJECT"),(0,i.kt)("li",{parentName:"ul"},"github.com/OWNER/PROJECT"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("a",{parentName:"li",href:"https://www.github.com/OWNER/PROJECT"},"https://www.github.com/OWNER/PROJECT"))),(0,i.kt)("p",null,"One example of such an extension is Valor. So, going back to the example above, installing it is like this:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension install gojek/optimus-extension-valor@v0.0.4\n")),(0,i.kt)("p",null,"or"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension install github.com/gojek/optimus-extension-valor@v0.0.4\n")),(0,i.kt)("p",null,"or"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension install https://github.com/gojek/optimus-extension-valor@v0.0.4\n")),(0,i.kt)("p",null,"Installation process is then in progress. If the installation is a success, the user can show it by running:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus --help\n")),(0,i.kt)("p",null,"A new command named after the extension will be available. For example, if the extension name is ",(0,i.kt)("em",{parentName:"p"},"optimus-extension-valor"),",\nthen by default the command named valor will be available. If the user wishes to change it, they can use ",(0,i.kt)("em",{parentName:"p"},"--alias"),"\nduring installation, or ",(0,i.kt)("em",{parentName:"p"},"rename")," it (explained later)."),(0,i.kt)("p",null,"The following is an example when running Optimus (without any command):"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"...\nAvailable Commands:\n  ...\n  extension   Operate with extension\n  ...\n  valor       Execute gojek/optimus-extension-valor [v0.0.4] extension\n  version     Print the client version information\n...\n")),(0,i.kt)("h3",{id:"executing"},"Executing"),(0,i.kt)("p",null,"In order to execute an extension, make sure to follow the installation process described above. After installation\nis finished, simply run the extension with the following command:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus [extension name or alias]\n")),(0,i.kt)("p",null,"Example of valor:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus valor\n")),(0,i.kt)("p",null,"Operation\nThe user can do some operations to an extension. This section explains more about the available commands.\nDo note that these commands are available on the installed extensions. For more detail, run the following command:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension [extension name or alias]\n")),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor\n\nThe above command shows all available commands for valor extension.\n\nOutput:\n\nSub-command to operate over extension [gojek/optimus-extension-valor@v0.0.4]\n\nUSAGE\n  optimus extension valor [flags]\n\nCORE COMMANDS\n  activate    activate is a sub command to allow user to activate an installed tag\n  describe    describe is a sub command to allow user to describe extension\n  rename      rename is a sub command to allow user to rename an extension command\n  uninstall   uninstall is a sub command to allow user to uninstall a specified tag of an extension\n  upgrade     upgrade is a sub command to allow user to upgrade an extension command\n\nINHERITED FLAGS\n      --help       Show help for command\n      --no-color   Disable colored output\n  -v, --verbose    if true, then more message will be provided if error encountered\n")),(0,i.kt)("h3",{id:"activate"},"Activate"),(0,i.kt)("p",null,"Activate a specific tag when running extension. For example, if the user has two versions of valor,\nwhich is v0.0.1 and v0.0.2, then by specifying the correct tag, the user can just switch between tag."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor activate v0.0.1\n")),(0,i.kt)("h3",{id:"describe"},"Describe"),(0,i.kt)("p",null,"Describes general information about an extension, such information includes all available releases of an extension\nin the local, which release is active, and more."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor describe\n")),(0,i.kt)("h3",{id:"rename"},"Rename"),(0,i.kt)("p",null,"Rename a specific extension to another command that is not reserved. By default, Optimus tries to figure out the\nappropriate command name from its project name. However, sometimes the extension name is not convenient like it\nbeing too long or the user just wants to change it."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor rename vl\n")),(0,i.kt)("h3",{id:"uninstall"},"Uninstall"),(0,i.kt)("p",null,"Uninstalls extension as a whole or only a specific tag. This allows the user to do some cleanup to preserve some\nstorage or to resolve some issues. By default, Optimus will uninstall the extension as a whole. To target a specific tag,\nuse the flag --tag."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor uninstall\n")),(0,i.kt)("h3",{id:"upgrade"},"Upgrade"),(0,i.kt)("p",null,"Upgrade allows the user to upgrade a certain extension to its latest tag. Although the user can use the install command,\nusing this command is shorter and easier as the user only needs to specify the installed extension."),(0,i.kt)("p",null,"Example:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-shell"},"$ optimus extension valor upgrade\n")))}c.isMDXComponent=!0}}]);