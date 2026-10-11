1 本项目就是类似https://github.com/openai/symphony里面的Orchestrator这个组件，专门负责Agent和management 直接的协调者
2 不同的是本项目可以直接用命令来创建启动agent（工具我做了：https://github.com/JoeWest9966/Nous-Warehouse我们将会把他开发为Orchestrator的可插拔插件）
3 API/CLI指的是本项目的为了也会被开发为插件，因为真正的面板是要用此项目实现https://github.com/JoeWest9966/Farm(我还没开发)
FARM就是我最终的AFK 架构决定是前后端都是rust 你了解下就行 本项目将是Farm的组件
4 Agent的实现是本项目触发
5 management目前仅作github 未来会增加别的